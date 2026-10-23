//go:build kind_e2e

// Copyright 2026 Mert Öztürk
// SPDX-License-Identifier: Apache-2.0

package kind_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/mertdotcc/kubectl-cron/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// isolatedNode is the control-plane container of this run's kind cluster;
// features that restart or read it refuse any other target.
func (s *suite) isolatedNode(t *testing.T) string {
	t.Helper()
	if want := strings.TrimPrefix(s.context, "kind-") + "-control-plane"; s.node != want {
		t.Fatalf("refusing node %q; expected the isolated %q", s.node, want)
	}
	return s.node
}

// ownedJob is a manual Job controlled by cj, as kubectl create job --from makes.
func (s *suite) ownedJob(cj batchv1.CronJob, name string, edit func(*batchv1.Job)) batchv1.Job {
	controller := true
	job := batchv1.Job{TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cj.Namespace, Annotations: map[string]string{"cronjob.kubernetes.io/instantiate": "manual"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "CronJob", Name: cj.Name, UID: cj.UID, Controller: &controller}}}, Spec: *cj.Spec.JobTemplate.Spec.DeepCopy()}
	edit(&job)
	return job
}

func (s *suite) liveCronJob(ns, name string) batchv1.CronJob {
	var cj batchv1.CronJob
	if err := json.Unmarshal([]byte(s.must(s.k("-n", ns, "get", "cronjob", name, "-o", "json")).Out), &cj); err != nil {
		s.t.Fatal(err)
	}
	return cj
}

func symbols(out string) string {
	m := regexp.MustCompile(`(?m)^\S+ \S+ \|([^|]*)\| `).FindAllStringSubmatch(out, -1)
	row := ""
	for _, line := range m {
		row += line[1]
	}
	return row
}

func (s *suite) timeline(t *testing.T) {
	ns := "cron-e2e-states"
	s.apply(corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: ns}})
	defer func() {
		s.k("-n", ns, "delete", "pods", "--all", "--grace-period=0", "--force")
		s.must(s.k("delete", "namespace", ns, "--wait=true", "--timeout=120s"))
	}()
	s.apply(s.cron("states", ns, "0 0 1 1 *", "UTC", `echo state-complete`, true), s.cron("quiet", ns, "0 0 1 1 *", "UTC", `true`, true))
	cj := s.liveCronJob(ns, "states")
	five, grace, zero := int64(5), int64(120), int32(0)
	// Creation times have second resolution, so space the Jobs out to fix their order.
	for _, job := range []batchv1.Job{
		s.ownedJob(cj, "held", func(j *batchv1.Job) { yes := true; j.Spec.Suspend = &yes }),
		s.ownedJob(cj, "done", func(*batchv1.Job) {}),
		s.ownedJob(cj, "waiting", func(j *batchv1.Job) { j.Spec.Parallelism = &zero }),
		s.ownedJob(cj, "stopping", func(j *batchv1.Job) {
			j.Spec.ActiveDeadlineSeconds = &five
			j.Spec.Template.Spec.TerminationGracePeriodSeconds = &grace
			j.Spec.Template.Spec.Containers[0].Command = []string{"sh", "-c", `trap "" TERM; sleep 600`}
		}),
	} {
		s.apply(job)
		time.Sleep(1100 * time.Millisecond)
	}
	s.k("-n", ns, "wait", "--for=condition=complete", "job/done", "--timeout=90s")
	s.wait("a terminating Job", 90*time.Second, func() bool {
		return strings.Contains(s.k("-n", ns, "get", "job", "stopping", "-o", "jsonpath={.status.conditions[?(@.type==\"FailureTarget\")].status}").Out, "True")
	})
	r := s.must(s.cli("-n", ns, "timeline", "states", "--color=never"))
	if got := symbols(r.Out); got != "s+.~" {
		t.Fatalf("timeline symbols %q, want suspended, complete, pending, terminating in creation order:\n%s", got, r.Out)
	}
	for _, want := range []string{"s suspended Job", "Shown entries: complete=1 terminating=1 pending=1 suspended-job=1\n"} {
		if !strings.Contains(r.Out, want) {
			t.Fatalf("missing %q:\n%s", want, r.Out)
		}
	}
	states := decodeReport(t, s.cli("-n", ns, "timeline", "states", "-o", "json"))
	got := []string{}
	for _, e := range states.History.Entries {
		got = append(got, e.State)
	}
	if states.Kind != "CronJobTimeline" || strings.Join(got, ",") != "terminating,pending,complete,suspended" {
		t.Fatalf("Report states changed: %v", got)
	}
	if empty := s.must(s.cli("-n", ns, "timeline", "quiet", "--color=never")); !strings.Contains(empty.Out, "No entries in this window.") || !strings.Contains(empty.Out, "Shown entries: none\n") {
		t.Fatalf("empty timeline:\n%s", empty.Out)
	}
	for name, symbol := range map[string]string{"slow": ">", "external": "E", "manual-only": "x", "failure": "x", "success": "+"} {
		if row := symbols(s.must(s.cli("timeline", name, "--limit=1000", "--color=never")).Out); !strings.Contains(row, symbol) {
			t.Fatalf("%s timeline lacks %q: %q", name, symbol, row)
		}
	}
	if row := symbols(s.must(s.cli("timeline", "manual-only", "--color=never", "--limit=1000")).Out); !strings.Contains(row, "?") {
		t.Fatal("projected unknown slots missing")
	}
	limited := s.must(s.cli("timeline", "manual-only", "--limit=2", "--color=never"))
	if len(symbols(limited.Out)) != 2 || !strings.Contains(limited.Err, "truncated") {
		t.Fatalf("timeline truncation: %+v", limited)
	}
	runs := decodeReport(t, s.cli("runs", "manual-only", "--limit=1000", "-o", "json"))
	drawn := decodeReport(t, s.cli("timeline", "manual-only", "--limit=1000", "-o", "json"))
	if len(runs.History.Entries) != len(drawn.History.Entries) || *runs.History.RetainedRuns != *drawn.History.RetainedRuns {
		t.Fatal("timeline and runs disagree about the same evidence")
	}
	clause := decodeReport(t, s.cli("-n", ns, "diagnose", "states", "-o", "json"))
	manual := 0
	for _, f := range (*clause.Diagnostics)[0].Findings {
		if f.Rule == "controller-warning-event" && strings.Contains(f.Message, "UnexpectedJob") && strings.Contains(f.Message, "The named Job is a retained owned manual Run.") {
			manual++
		}
	}
	if manual == 0 {
		t.Fatal("UnexpectedJob Findings for owned manual Runs lack the manual-Run clause")
	}
	for _, args := range [][]string{{"--since=0"}, {"--since=8761h"}, {"--limit=0"}, {"--limit=1001"}, {"-f", "/dev/null"}} {
		if got := s.cli(append([]string{"timeline", "failure"}, args...)...); got.Code != 1 || got.Out != "" {
			t.Fatalf("invalid timeline arguments accepted: %v", args)
		}
	}
}

func (s *suite) calendar(t *testing.T) {
	r := s.cli("calendar", "--next=10m", "--exit-on-findings", "-o", "json")
	var report model.Report
	if r.Code != 2 || json.Unmarshal([]byte(r.Out), &report) != nil || report.Calendar == nil || report.Source != "cluster" {
		t.Fatalf("live calendar with shared minute slots: %+v", r)
	}
	if len(report.Calendar.Findings) != 1 || report.Calendar.Findings[0].Rule != "coincident-schedules" {
		t.Fatalf("coincident-schedules missing: %+v", report.Calendar.Findings)
	}
	human := s.must(s.cli("calendar", "--next=10m", "--color=never"))
	if !strings.Contains(human.Out, "COUNT") || !strings.Contains(human.Err, "excluded from calculation (suspended)") {
		t.Fatalf("calendar table or exclusions: %+v", human)
	}
	all := decodeReport(t, s.cli("calendar", "-A", "--next=10m", "--no-findings", "-o", "json"))
	namespaces := map[string]bool{}
	for _, cj := range all.Calendar.CronJobs {
		namespaces[cj.Namespace] = true
	}
	if len(namespaces) < 3 || len(all.Calendar.Findings) != 0 {
		t.Fatalf("calendar -A or --no-findings: %v", namespaces)
	}
	named := decodeReport(t, s.cli("calendar", "success", "--next=10m", "-o", "json"))
	for _, cj := range named.Calendar.CronJobs {
		if cj.Name != "success" {
			t.Fatalf("named calendar included %s", cj.Name)
		}
	}
	if wide := s.must(s.cli("calendar", "--next=10m", "-o", "wide", "--color=never")); !strings.Contains(wide.Out, "* * * * *") {
		t.Fatal("wide calendar lacks expressions")
	}
	var yamlReport model.Report
	if err := yaml.Unmarshal([]byte(s.must(s.cli("calendar", "--next=10m", "-o", "yaml")).Out), &yamlReport); err != nil || yamlReport.Kind != "CronJobCalendar" {
		t.Fatalf("calendar YAML: %v", err)
	}
}

func frame(line string) model.Report {
	var r model.Report
	json.Unmarshal([]byte(line), &r)
	return r
}

func rowNames(r model.Report) map[string]bool {
	names := map[string]bool{}
	for _, row := range r.Rows() {
		names[row.Name] = true
	}
	return names
}

func (s *suite) watch(t *testing.T) {
	ns := "cron-e2e-watch"
	s.apply(corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: ns}})
	defer func() { s.must(s.k("delete", "namespace", ns, "--wait=true", "--timeout=120s")) }()
	s.k("-n", ns, "delete", "cronjobs", "--all")
	streaming := func(r model.Report) bool {
		return r.Watch != nil && r.Watch.Mode == "api" && r.Watch.State == "streaming"
	}
	p := s.start(t, "-n", ns, "--watch", "--watch-interval=250ms", "-o", "json")
	p.line(t, func(l string) bool { r := frame(l); return streaming(r) && len(r.Rows()) == 0 })
	s.apply(s.cron("live", ns, "*/5 * * * *", "UTC", "true", false))
	p.line(t, func(l string) bool { return rowNames(frame(l))["live"] })
	s.must(s.k("-n", ns, "patch", "cronjob", "live", "-p", `{"spec":{"suspend":true,"schedule":"0 3 * * *"}}`))
	p.line(t, func(l string) bool {
		r := frame(l)
		return len(r.Rows()) == 1 && r.Rows()[0].Suspended && r.Rows()[0].Schedule.Expression == "0 3 * * *"
	})
	s.must(s.k("-n", ns, "delete", "cronjob", "live"))
	p.line(t, func(l string) bool {
		r := frame(l)
		return len(r.Rows()) == 0 && r.Summary != nil && r.Summary.Total == 0
	})
	s.stop(t, p)
	if p.notes.Len() != 0 {
		t.Fatalf("healthy watch printed notices: %s", p.notes.String())
	}
	var previous time.Time
	for _, l := range strings.Split(strings.TrimSpace(p.out.String()), "\n") {
		if !strings.HasPrefix(l, "{") || !strings.HasSuffix(l, "}") {
			t.Fatalf("frame is not one compact JSON object: %q", l)
		}
		// Each snapshot has its own evaluation instant.
		if at := frame(l).GeneratedAt; !at.After(previous) {
			t.Fatalf("frame evaluation time %s did not advance past %s", at, previous)
		} else {
			previous = at
		}
	}

	named := s.start(t, "-n", ns, "later", "--watch", "--watch-interval=250ms", "-o", "json")
	named.line(t, func(l string) bool { r := frame(l); return streaming(r) && len(r.Rows()) == 0 })
	s.apply(s.cron("noise", ns, "*/5 * * * *", "UTC", "true", false))
	s.apply(s.cron("later", ns, "*/5 * * * *", "UTC", "true", false))
	named.line(t, func(l string) bool { r := frame(l); return len(r.Rows()) == 1 && rowNames(r)["later"] })
	s.stop(t, named)

	team := s.cron("labelled", ns, "*/5 * * * *", "UTC", "true", false)
	team.Labels["team"] = "a"
	selected := s.start(t, "-n", ns, "-l", "team=a", "--watch", "--watch-interval=250ms", "-o", "json")
	selected.line(t, func(l string) bool { r := frame(l); return streaming(r) && len(r.Rows()) == 0 })
	s.apply(team)
	selected.line(t, func(l string) bool { r := frame(l); return len(r.Rows()) == 1 && rowNames(r)["labelled"] })
	s.stop(t, selected)

	for _, tc := range []struct {
		args  []string
		match func(model.Report) bool
	}{
		{[]string{"--summary=only"}, func(r model.Report) bool { return r.CronJobs == nil && r.Summary != nil && r.Summary.Total == 15 }},
		{[]string{"--problems"}, func(r model.Report) bool { return len(r.Rows()) == 2 }},
		{[]string{"-A"}, func(r model.Report) bool {
			ns := map[string]bool{}
			for _, row := range r.Rows() {
				ns[row.Namespace] = true
			}
			return len(ns) >= 4
		}},
	} {
		w := s.start(t, append([]string{"--watch", "--watch-interval=250ms", "-o", "json"}, tc.args...)...)
		w.line(t, func(l string) bool { r := frame(l); return streaming(r) && tc.match(r) })
		s.stop(t, w)
	}

	for format, check := range map[string]func(string) bool{
		"yaml": func(out string) bool {
			docs := strings.Split(strings.TrimPrefix(out, "---\n"), "\n---\n")
			for _, doc := range docs {
				var r model.Report
				if yaml.Unmarshal([]byte(doc), &r) != nil || r.Kind != "CronJobReport" || r.Watch == nil {
					return false
				}
			}
			return strings.HasPrefix(out, "---\n") && len(docs) > 3
		},
		"table": func(out string) bool {
			return strings.Count(out, "| watch streaming\n") > 3 && !strings.Contains(out, "\x1b[")
		},
		"wide": func(out string) bool { return strings.Contains(out, "TIMEZONE") },
	} {
		w := s.start(t, "-n", ns, "--watch", "--watch-interval=250ms", "-o", format)
		time.Sleep(2 * time.Second)
		s.stop(t, w)
		if !check(w.out.String()) {
			t.Fatalf("%s watch frames:\n%s", format, w.out.String())
		}
	}

	exit := s.cli("--watch", "--exit-on-findings", "-o", "json")
	if exit.Code != 2 || strings.Count(exit.Out, "\n") != 1 {
		t.Fatalf("watch --exit-on-findings: %+v", exit)
	}
	for _, args := range [][]string{{"--watch-interval=249ms"}, {"--watch-interval=61s"}, {"--no-findings", "--exit-on-findings"}, {"-f", "/dev/null"}} {
		if got := s.cli(append([]string{"--watch"}, args...)...); got.Code != 1 || got.Out != "" {
			t.Fatalf("invalid watch arguments accepted: %v", args)
		}
	}

	// Refreshes come from the informer cache: one list and one watch serve every frame.
	polled := s.start(t, "-n", ns, "--watch", "--watch-interval=250ms", "-o", "json", "-v=6")
	time.Sleep(4 * time.Second)
	s.stop(t, polled)
	lists := regexp.MustCompile(`verb="GET" url="[^"]*/cronjobs\?limit=500"`).FindAllString(polled.notes.String(), -1)
	watches := regexp.MustCompile(`verb="GET" url="[^"]*/cronjobs\?[^"]*watch=true`).FindAllString(polled.notes.String(), -1)
	if len(lists) != 1 || len(watches) != 1 || strings.Count(polled.out.String(), "\n") < 10 {
		t.Fatalf("watch polled the API: lists=%d watches=%d frames=%d", len(lists), len(watches), strings.Count(polled.out.String(), "\n"))
	}
}

func (s *suite) access(t *testing.T) {
	sa := func(name string) corev1.ServiceAccount {
		return corev1.ServiceAccount{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.ns}}
	}
	role := func(name string, verbs []string, resources ...string) rbacv1.Role {
		return rbacv1.Role{TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.ns}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{"batch"}, Resources: resources, Verbs: verbs}}}
	}
	binding := func(name string) rbacv1.RoleBinding {
		return rbacv1.RoleBinding{TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.ns}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: s.ns}}}
	}
	read := []string{"get", "list", "watch"}
	s.apply(sa("e2e-none"), sa("e2e-listonly"), sa("e2e-viewer"), sa("e2e-partial"),
		role("e2e-listonly", []string{"get", "list"}, "cronjobs"), role("e2e-viewer", read, "cronjobs"), role("e2e-partial", read, "cronjobs", "jobs"),
		binding("e2e-listonly"), binding("e2e-viewer"), binding("e2e-partial"))
	as := func(who string, args ...string) result {
		return s.cli(append(args, "--as=system:serviceaccount:"+s.ns+":"+who)...)
	}
	s.wait("RBAC propagation", 30*time.Second, func() bool { return as("e2e-viewer", "-o", "json").Code == 0 })
	for _, args := range [][]string{{}, {"--watch", "-o", "json"}, {"calendar"}} {
		if got := as("e2e-none", args...); got.Code != 1 || got.Out != "" || !strings.Contains(got.Err, "forbidden") {
			t.Fatalf("no access %v: %+v", args, got)
		}
	}
	if got := as("e2e-listonly", "--watch", "-o", "json"); got.Code != 1 || !strings.Contains(got.Err, "cannot watch") {
		t.Fatalf("missing watch permission must stop before streaming: %+v", got)
	}
	if got := as("e2e-listonly", "-o", "json"); got.Code != 0 {
		t.Fatalf("list-only Overview: %+v", got)
	}

	inspection := decodeReport(t, as("e2e-viewer", "inspect", "failure", "-o", "json"))
	if inspection.Inspection.RetainedRuns != nil {
		t.Fatal("forbidden Jobs read became a zero count")
	}
	required := as("e2e-viewer", "inspect", "failure", "--require-evidence", "-o", "json")
	if required.Code != 1 || !strings.Contains(required.Out, `"kind": "CronJobInspection"`) || !strings.Contains(required.Err, "required evidence incomplete: jobs forbidden (permission-denied); events forbidden (permission-denied)") {
		t.Fatalf("inspect --require-evidence: %+v", required)
	}
	history := decodeReport(t, as("e2e-viewer", "runs", "failure", "-o", "json"))
	for _, e := range history.History.Entries {
		if e.State != "unknown" {
			t.Fatal("forbidden Jobs produced an observed state")
		}
	}
	if got := as("e2e-viewer", "logs", "failure"); got.Code != 1 || got.Out != "" {
		t.Fatalf("logs without Jobs access: %+v", got)
	}
	gap := as("e2e-viewer", "diagnose", "failure", "--exit-on-findings")
	if gap.Code != 0 || !strings.Contains(gap.Err, "jobs evidence forbidden (permission-denied).") || regexp.MustCompile(`(?m) +$`).MatchString(gap.Err) {
		t.Fatalf("diagnose without evidence: %+v", gap)
	}
	closed := as("e2e-viewer", "diagnose", "--exit-on-findings", "--require-evidence")
	if closed.Code != 1 || !strings.Contains(closed.Err, "jobs forbidden (permission-denied) for 15 CronJobs; events forbidden (permission-denied) for 15 CronJobs") {
		t.Fatalf("diagnose --require-evidence: %+v", closed)
	}
	if got := as("e2e-viewer", "diagnose", "-A"); got.Code != 1 {
		t.Fatal("namespace-scoped identity listed every namespace")
	}
	partial := as("e2e-partial", "diagnose", "failure", "--exit-on-findings", "--require-evidence", "-o", "json")
	if partial.Code != 1 || !strings.Contains(partial.Out, "retained-job-failures") || !strings.Contains(partial.Err, "required evidence incomplete: events forbidden (permission-denied)") || strings.Contains(partial.Err, "jobs forbidden") {
		t.Fatalf("partial evidence: %+v", partial)
	}
	if got := s.cli("diagnose", "-A", "--require-evidence", "--exit-on-findings", "-o", "json"); got.Code != 2 {
		t.Fatalf("complete evidence must leave the Findings exit: %+v", got)
	}
}

// ptyRun runs the CLI on a pseudo-terminal, interrupting it after a duration
// when one is given. Python's pty module is portable across macOS and Linux.
const ptyRun = `
import os, pty, select, signal, subprocess, sys, time
duration, cmd = float(sys.argv[1]), sys.argv[2:]
master, slave = pty.openpty()
proc = subprocess.Popen(cmd, stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
os.close(slave)
out, deadline, sent = b"", time.time() + duration, False
while True:
    if duration > 0 and not sent and time.time() >= deadline:
        proc.send_signal(signal.SIGINT)
        sent = True
    ready = select.select([master], [], [], 0.1)[0]
    data = b""
    if ready:
        try:
            data = os.read(master, 65536)
        except OSError:
            pass
        out += data
    if (ready and not data) or (not ready and proc.poll() is not None):
        break
sys.stdout.buffer.write(out)
sys.exit(proc.wait())
`

func (s *suite) onTerminal(t *testing.T, duration float64, env []string, args ...string) string {
	t.Helper()
	cmd := exec.Command("python3", append([]string{"-I", "-c", ptyRun, fmt.Sprint(duration), s.binary, "--kubeconfig", s.kubeconfig, "--context", s.context}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("terminal run %v %v: %v", env, args, err)
	}
	return strings.ReplaceAll(string(out), "\r", "")
}

func (s *suite) tty(t *testing.T) {
	for _, tc := range []struct {
		env   []string
		args  []string
		color bool
	}{
		{[]string{"TERM=xterm"}, []string{"--color=auto"}, true},
		{[]string{"TERM=xterm", "NO_COLOR=1"}, []string{"--color=auto"}, false},
		{[]string{"TERM=dumb"}, []string{"--color=auto"}, false},
		{[]string{"TERM=xterm", "NO_COLOR=1"}, []string{"--color=always"}, true},
		{[]string{"TERM=xterm"}, []string{"--color=always", "-o", "json"}, false},
	} {
		if got := strings.Contains(s.onTerminal(t, 0, tc.env, tc.args...), "\x1b["); got != tc.color {
			t.Fatalf("terminal %v %v colored=%t", tc.env, tc.args, got)
		}
	}
	redraw := s.onTerminal(t, 2, []string{"TERM=xterm"}, "--watch", "--watch-interval=250ms", "--color=never")
	if !strings.Contains(redraw, "\x1b[H\x1b[2J") {
		t.Fatal("terminal watch did not redraw in place")
	}
	appended := s.onTerminal(t, 2, []string{"TERM=dumb"}, "--watch", "--watch-interval=250ms", "--color=never")
	if strings.Contains(appended, "\x1b[H") || strings.Count(appended, "Observed CronJobs |") < 4 {
		t.Fatal("TERM=dumb watch must append frames without redrawing")
	}
}

func (s *suite) plugin(t *testing.T) {
	dir := t.TempDir()
	data, err := os.ReadFile(s.binary)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "kubectl-cron"), data, 0755); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, error) {
		cmd := exec.Command("kubectl", args...)
		cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"))
		out, err := cmd.Output()
		return string(out), err
	}
	if out, err := run("plugin", "list"); err != nil || !strings.Contains(out, filepath.Join(dir, "kubectl-cron")) {
		t.Fatalf("kubectl plugin list: %v\n%s", err, out)
	}
	out, err := run("cron", "--kubeconfig", s.kubeconfig, "--context", s.context, "-o", "json")
	var report model.Report
	if err != nil || json.Unmarshal([]byte(out), &report) != nil || report.Kind != "CronJobReport" || len(report.Rows()) != 15 {
		t.Fatalf("kubectl cron: %v\n%s", err, out)
	}
}

func (s *suite) restart(t *testing.T) {
	node := s.isolatedNode(t)
	p := s.start(t, "--watch", "--watch-interval=1s", "-o", "json", "-v=6")
	p.line(t, func(l string) bool { r := frame(l); return r.Watch != nil && r.Watch.State == "streaming" })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "exec", node, "sh", "-c", "crictl stop $(crictl ps --name kube-apiserver -q)").CombinedOutput(); err != nil {
		t.Fatalf("stop API server: %v %s", err, out)
	}
	p.lineWithin(t, 60*time.Second, func(l string) bool { r := frame(l); return r.Watch != nil && r.Watch.State != "streaming" })
	time.Sleep(2 * time.Second)
	s.wait("API server readiness", 4*time.Minute, func() bool { return s.k("get", "--raw", "/readyz").Code == 0 })
	ready := time.Now()
	p.lineWithin(t, 30*time.Second, func(l string) bool {
		r := frame(l)
		return r.Watch != nil && r.Watch.State == "streaming" && r.GeneratedAt.After(ready)
	})
	select {
	case err := <-p.done:
		t.Fatalf("watch exited across the restart: %v %s", err, p.notes.String())
	default:
	}
	s.stop(t, p)
	trace := p.notes.String()
	if regexp.MustCompile(`cronjobs\?limit=[0-9]+&resourceVersion=[1-9]`).MatchString(trace) {
		t.Fatal("relist paginated a specific resource version (an Exact read)")
	}
	notices := []string{}
	for _, line := range strings.Split(trace, "\n") {
		if strings.HasPrefix(line, "error:") {
			t.Fatalf("fatal error across the restart: %s", line)
		}
		if strings.HasPrefix(line, "CronJob watch reconnecting: ") {
			if len(notices) > 0 && notices[len(notices)-1] == line {
				t.Fatalf("repeated identical notice: %s", line)
			}
			notices = append(notices, line)
		}
	}
	for _, l := range strings.Split(strings.TrimSpace(p.out.String()), "\n") {
		if r := frame(l); r.Watch != nil && r.Watch.State == "reconnecting" && len(r.Rows()) != 15 {
			t.Fatal("cached rows disappeared while reconnecting")
		}
	}
}

func (s *suite) readOnly(t *testing.T) {
	node := s.isolatedNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log, err := exec.CommandContext(ctx, "docker", "exec", node, "cat", "/var/log/cron-audit/audit.log").Output()
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"cronjobs": true, "jobs": true, "events": true, "pods": true, "pods/log": true}
	requests := 0
	scanner := bufio.NewScanner(bytes.NewReader(log))
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		var e struct {
			Stage, Verb, UserAgent, RequestURI string
			ObjectRef                          *struct{ Resource, Subresource string }
		}
		if json.Unmarshal(scanner.Bytes(), &e) != nil || e.Stage != "ResponseComplete" || !strings.HasPrefix(e.UserAgent, "kubectl-cron") {
			continue
		}
		requests++
		if e.Verb != "get" && e.Verb != "list" && e.Verb != "watch" {
			t.Fatalf("plugin issued %s %s", e.Verb, e.RequestURI)
		}
		resource := ""
		if e.ObjectRef != nil {
			resource = e.ObjectRef.Resource
			if e.ObjectRef.Subresource != "" {
				resource += "/" + e.ObjectRef.Subresource
			}
		}
		if !allowed[resource] {
			t.Fatalf("plugin read an undocumented resource: %s", e.RequestURI)
		}
	}
	if err := scanner.Err(); err != nil || requests < 100 {
		t.Fatalf("audit log covered %d plugin requests: %v", requests, err)
	}
}
