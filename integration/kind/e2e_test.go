//go:build kind_e2e

// Copyright 2026 Mert Öztürk
// SPDX-License-Identifier: Apache-2.0

// Package kind_test verifies the built CLI against an explicitly isolated cluster.
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
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mertdotcc/kubectl-cron/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"
)

type suite struct {
	t                                                   *testing.T
	binary, kubeconfig, context, artifacts, image, node string
	ns, other, quota                                    string
	sequence                                            atomic.Int32
}

type result struct {
	Out, Err string
	Code     int
}

func TestKindEndToEnd(t *testing.T) {
	s := &suite{t: t, binary: os.Getenv("KUBECTL_CRON_E2E_BINARY"), kubeconfig: os.Getenv("KUBECTL_CRON_E2E_KUBECONFIG"), context: os.Getenv("KUBECTL_CRON_E2E_CONTEXT"), artifacts: os.Getenv("KUBECTL_CRON_E2E_ARTIFACTS"), image: os.Getenv("KUBECTL_CRON_E2E_IMAGE"), node: os.Getenv("KUBECTL_CRON_E2E_NODE"), ns: "cron-e2e-main", other: "cron-e2e-other", quota: "cron-e2e-quota"}
	if s.binary == "" || s.kubeconfig == "" || s.artifacts == "" || !strings.HasPrefix(s.context, "kind-cron-e2e-") {
		t.Fatal("run via hack/test-kind.sh; an explicit binary, kubeconfig, artifact directory, and isolated kind context are required")
	}
	if s.image == "" {
		s.image = "busybox:1.37.0"
	}
	if err := os.MkdirAll(s.artifacts, 0700); err != nil {
		t.Fatal(err)
	}
	server := s.k("config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}").Out
	if !strings.HasPrefix(server, "https://127.0.0.1:") {
		t.Fatalf("refusing non-local API server %q", server)
	}
	s.must(s.k("config", "set-context", s.context, "--namespace", s.ns))
	if os.Getenv("KUBECTL_CRON_E2E_REUSE_FIXTURES") != "1" {
		s.setup()
	}
	s.feature(t, "overview", s.overview)
	s.feature(t, "inspect", s.inspect)
	s.feature(t, "runs", s.runs)
	s.feature(t, "logs", s.logs)
	s.feature(t, "diagnose", s.diagnose)
	s.feature(t, "timeline", s.timeline)
	s.feature(t, "calendar", s.calendar)
	s.feature(t, "watch", s.watch)
	s.feature(t, "access", s.access)
	s.feature(t, "terminal", s.tty)
	s.feature(t, "plugin", s.plugin)
	s.feature(t, "restart", s.restart)
	// Last, so it covers every request the features above made.
	s.feature(t, "read-only", s.readOnly)
}

func (s *suite) feature(t *testing.T, name string, test func(*testing.T)) {
	t.Run(name, func(child *testing.T) { parent := s.t; s.t = child; defer func() { s.t = parent }(); test(child) })
}

func (s *suite) command(input string, program string, args ...string) result {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Stdin = strings.NewReader(input)
	var out, notes bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &notes
	err := cmd.Run()
	code := 0
	if err != nil {
		code = 1
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		}
		if ctx.Err() != nil {
			notes.WriteString(ctx.Err().Error())
		}
	}
	id := s.sequence.Add(1)
	prefix := filepath.Join(s.artifacts, fmt.Sprintf("%04d", id))
	for suffix, data := range map[string][]byte{".stdout": out.Bytes(), ".stderr": notes.Bytes()} {
		if e := os.WriteFile(prefix+suffix, data, 0600); e != nil {
			s.t.Error(e)
		}
	}
	record, _ := json.Marshal(map[string]any{"program": program, "args": args, "exitCode": code})
	if e := os.WriteFile(prefix+".command.json", record, 0600); e != nil {
		s.t.Error(e)
	}
	return result{Out: out.String(), Err: notes.String(), Code: code}
}
func (s *suite) k(args ...string) result {
	return s.command("", "kubectl", append([]string{"--kubeconfig", s.kubeconfig, "--context", s.context}, args...)...)
}
func (s *suite) kn(args ...string) result { return s.k(append([]string{"-n", s.ns}, args...)...) }
func (s *suite) cli(args ...string) result {
	return s.command("", s.binary, append([]string{"--kubeconfig", s.kubeconfig, "--context", s.context}, args...)...)
}
func (s *suite) must(r result) result {
	s.t.Helper()
	if r.Code != 0 {
		s.t.Fatalf("command failed (%d): %s\n%s", r.Code, r.Err, r.Out)
	}
	return r
}
func (s *suite) apply(objects ...any) {
	data, err := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "List", "items": objects})
	if err != nil {
		s.t.Fatal(err)
	}
	s.must(s.command(string(data), "kubectl", "--kubeconfig", s.kubeconfig, "--context", s.context, "apply", "-f", "-"))
}
func (s *suite) wait(label string, timeout time.Duration, ready func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Second)
	}
	s.t.Fatalf("timed out waiting for %s", label)
}
func (s *suite) job(name string) batchv1.Job {
	r := s.must(s.kn("get", "job", name, "-o", "json"))
	var job batchv1.Job
	if err := json.Unmarshal([]byte(r.Out), &job); err != nil {
		s.t.Fatal(err)
	}
	return job
}
func (s *suite) terminal(name string, condition batchv1.JobConditionType) {
	s.wait(name+" "+string(condition), 150*time.Second, func() bool {
		for _, c := range s.job(name).Status.Conditions {
			if c.Type == condition && c.Status == corev1.ConditionTrue {
				return true
			}
		}
		return false
	})
}
func (s *suite) createRun(cj, name string) { s.must(s.kn("create", "job", "--from=cronjob/"+cj, name)) }
func decodeReport(t *testing.T, r result) model.Report {
	t.Helper()
	if r.Code != 0 {
		t.Fatalf("CLI exit %d: %s\n%s", r.Code, r.Err, r.Out)
	}
	var report model.Report
	if err := json.Unmarshal([]byte(r.Out), &report); err != nil {
		t.Fatalf("JSON: %v\n%s", err, r.Out)
	}
	if report.APIVersion != model.APIVersion {
		t.Fatal("missing report version")
	}
	return report
}

func (s *suite) cron(name, ns, schedule, zone, script string, suspended bool) batchv1.CronJob {
	zero, fifty := int32(0), int32(50)
	deadline, grace := int64(900), int64(1)
	cj := batchv1.CronJob{TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "CronJob"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"e2e": "primary"}}, Spec: batchv1.CronJobSpec{Schedule: schedule, Suspend: &suspended, ConcurrencyPolicy: batchv1.ForbidConcurrent, SuccessfulJobsHistoryLimit: &fifty, FailedJobsHistoryLimit: &fifty, JobTemplate: batchv1.JobTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"e2e": "job-template"}}, Spec: batchv1.JobSpec{BackoffLimit: &zero, ActiveDeadlineSeconds: &deadline, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"e2e": "pod-template"}}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, TerminationGracePeriodSeconds: &grace, Containers: []corev1.Container{{Name: "worker", Image: s.image, ImagePullPolicy: corev1.PullNever, Command: []string{"sh", "-c", script}, Env: []corev1.EnvVar{{Name: "JOB_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels['batch.kubernetes.io/job-name']"}}}}}}}}}}}}
	if zone != "" {
		cj.Spec.TimeZone = &zone
	}
	return cj
}

func (s *suite) setup() {
	for _, ns := range []string{s.ns, s.other, s.quota} {
		s.apply(corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: ns}})
	}
	marker := `echo "worker-$JOB_NAME"; echo second-line; echo third-line`
	success := s.cron("success", s.ns, "* * * * *", "UTC", marker, false)
	failure := s.cron("failure", s.ns, "* * * * *", "UTC", `echo scheduled-failure; exit 7`, false)
	slow := s.cron("slow", s.ns, "* * * * *", "UTC", `i=0; while [ "$i" -lt 400 ]; do echo "tick-$i"; i=$((i+1)); sleep 2; done`, false)
	manual := s.cron("manual-only", s.ns, "* * * * *", "UTC", `echo "worker-$JOB_NAME"; case "$JOB_NAME" in *failed) exit 7;; esac; echo second-line; echo third-line`, true)
	manual.Spec.JobTemplate.Spec.Template.Annotations = map[string]string{"kubectl.kubernetes.io/default-container": "sidecar"}
	manual.Spec.JobTemplate.Spec.Template.Spec.InitContainers = []corev1.Container{{Name: "setup", Image: s.image, ImagePullPolicy: corev1.PullNever, Command: []string{"sh", "-c", `echo "init-$JOB_NAME"`}, Env: manual.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env}}
	manual.Spec.JobTemplate.Spec.Template.Spec.Containers = append(manual.Spec.JobTemplate.Spec.Template.Spec.Containers, corev1.Container{Name: "sidecar", Image: s.image, ImagePullPolicy: corev1.PullNever, Command: []string{"sh", "-c", `echo "sidecar-$JOB_NAME"`}, Env: manual.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env})
	retry := s.cron("retry-pods", s.ns, "* * * * *", "UTC", `echo "retry-$JOB_NAME-$(cat /mode/value)"; test "$(cat /mode/value)" = success`, true)
	four := int32(4)
	retry.Spec.JobTemplate.Spec.BackoffLimit = &four
	retry.Spec.JobTemplate.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "mode", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "retry-mode"}}}}}
	retry.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "mode", MountPath: "/mode"}}
	restart := s.cron("restart-container", s.ns, "* * * * *", "UTC", `if [ ! -e /state/attempt ]; then touch /state/attempt; echo previous-container-attempt; exit 9; fi; echo recovered-container-attempt; sleep 800`, true)
	restart.Spec.JobTemplate.Spec.BackoffLimit = &four
	restart.Spec.JobTemplate.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
	restart.Spec.JobTemplate.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
	restart.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "state", MountPath: "/state"}}
	allow := s.cron("allow-two", s.ns, "* * * * *", "UTC", `echo concurrent-active; sleep 800`, true)
	allow.Spec.ConcurrencyPolicy = batchv1.AllowConcurrent
	long := s.cron("long-complete", s.ns, "* * * * *", "UTC", `echo long-start; sleep 65; echo long-complete`, true)
	external := s.cron("external", s.ns, "* * * * *", "UTC", marker, true)
	manager := "example.com/e2e-controller"
	external.Spec.JobTemplate.Spec.ManagedBy = &manager
	pruned := s.cron("pruned", s.ns, "* * * * *", "UTC", marker, false)
	pruned.Spec.SuccessfulJobsHistoryLimit = &zero32
	pruned.Spec.FailedJobsHistoryLimit = &zero32
	other := s.cron("success", s.other, "* * * * *", "Asia/Tokyo", marker, true)
	other.Labels["e2e"] = "secondary"
	objects := []any{success, failure, slow, manual, retry, restart, allow, long, external, pruned, other,
		s.cron("paused", s.ns, "0 6 * * *", "Europe/Berlin", marker, true), s.cron("timezone-unknown", s.ns, "* * * * *", "", marker, false),
		s.cron("impossible", s.ns, "0 0 31 2 *", "UTC", marker, false), s.cron("reused", s.ns, "* * * * *", "UTC", marker, true), s.cron("explain", s.ns, "@hourly", "UTC", marker, true),
		corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: "retry-mode", Namespace: s.ns}, Data: map[string]string{"value": "fail"}},
		corev1.ResourceQuota{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ResourceQuota"}, ObjectMeta: metav1.ObjectMeta{Name: "no-jobs", Namespace: s.quota}, Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourceName("count/jobs.batch"): resource.MustParse("0")}}}, s.cron("blocked", s.quota, "* * * * *", "UTC", marker, false)}
	s.apply(objects...)
	for _, pair := range [][2]string{{"slow", "slow-active"}, {"allow-two", "allow-first"}, {"allow-two", "allow-second"}, {"long-complete", "long-observed"}, {"external", "external-owned"}} {
		s.createRun(pair[0], pair[1])
	}
	for _, name := range []string{"manual-old", "manual-failed", "manual-new"} {
		s.createRun("manual-only", name)
		condition := batchv1.JobComplete
		if name == "manual-failed" {
			condition = batchv1.JobFailed
		}
		s.terminal(name, condition)
	}
	s.createRun("retry-pods", "retry-success")
	s.wait("a failed retry Pod", 90*time.Second, func() bool { return s.job("retry-success").Status.Failed > 0 })
	s.must(s.kn("patch", "configmap", "retry-mode", "--type=merge", "-p", `{"data":{"value":"success"}}`))
	s.terminal("retry-success", batchv1.JobComplete)
	s.createRun("restart-container", "restart-success")
	s.wait("restarted live container", 90*time.Second, func() bool {
		var pods corev1.PodList
		json.Unmarshal([]byte(s.kn("get", "pods", "-l", "batch.kubernetes.io/job-name=restart-success", "-o", "json").Out), &pods)
		for _, pod := range pods.Items {
			for _, c := range pod.Status.ContainerStatuses {
				if c.RestartCount > 0 && c.State.Running != nil {
					return true
				}
			}
		}
		return false
	})
	controller := true
	old := batchv1.Job{TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: metav1.ObjectMeta{Name: "reused-old-owner", Namespace: s.ns, Finalizers: []string{"e2e.kubectl.dev/retain"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "CronJob", Name: "reused", UID: types.UID("00000000-0000-0000-0000-000000000001"), Controller: &controller}}}, Spec: *success.Spec.JobTemplate.Spec.DeepCopy()}
	unowned := old.DeepCopy()
	unowned.Name = "manual-only-unowned"
	unowned.OwnerReferences = nil
	unowned.Finalizers = nil
	s.apply(old, *unowned)
	s.wait("a naturally scheduled success", 100*time.Second, func() bool {
		r := s.kn("get", "cronjob", "success", "-o", "json")
		var cj batchv1.CronJob
		json.Unmarshal([]byte(r.Out), &cj)
		return cj.Status.LastSuccessfulTime != nil
	})
}

var zero32 int32

func (s *suite) overview(t *testing.T) {
	r := decodeReport(t, s.cli("-o", "json"))
	if r.Kind != "CronJobReport" || len(r.Rows()) != 15 || r.Summary == nil || r.Summary.Total != 15 {
		t.Fatalf("unexpected selection: %+v", r)
	}
	byName := map[string]model.CronJob{}
	for _, row := range r.Rows() {
		byName[row.Name] = row
		if row.Namespace != s.ns {
			t.Fatal("default context namespace lost")
		}
	}
	if byName["paused"].Schedule.Next.Reason != "suspended" || byName["timezone-unknown"].Schedule.Next.Reason != "controller-timezone-unknown" || byName["impossible"].Schedule.Next.Reason != "no-next-occurrence" || byName["success"].LastSuccess.Time == nil {
		t.Fatal("Overview uncertainty or recorded state lost")
	}
	for _, format := range []string{"table", "wide", "yaml"} {
		result := s.must(s.cli("-o", format, "--color=never"))
		if strings.Contains(result.Out, "\x1b[") {
			t.Fatal("unexpected coloring")
		}
		if format == "yaml" {
			var report model.Report
			if err := yaml.Unmarshal([]byte(result.Out), &report); err != nil || len(report.Rows()) != 15 {
				t.Fatalf("YAML: %v", err)
			}
		}
	}
	for _, order := range []string{"name", "next", "last", "findings"} {
		sorted := decodeReport(t, s.cli("--sort="+order, "-o", "json"))
		if len(sorted.Rows()) != 15 {
			t.Fatal("sort changed scope")
		}
		if order == "name" {
			names := []string{}
			for _, row := range sorted.Rows() {
				names = append(names, row.Name)
			}
			if !sort.StringsAreSorted(names) {
				t.Fatal("names not sorted")
			}
		}
	}
	all := decodeReport(t, s.cli("-A", "-o", "json"))
	if len(all.Rows()) != 17 {
		t.Fatalf("all namespaces: %d", len(all.Rows()))
	}
	selected := decodeReport(t, s.cli("-A", "-l", "e2e=secondary", "-o", "json"))
	if len(selected.Rows()) != 1 || selected.Rows()[0].Namespace != s.other {
		t.Fatal("CronJob label selector or namespace scope lost")
	}
	named := decodeReport(t, s.cli("cronjob/explain", "-o", "json"))
	if len(named.Rows()) != 1 || named.Rows()[0].Name != "explain" {
		t.Fatal("reserved name escape failed")
	}
	assumed := decodeReport(t, s.cli("timezone-unknown", "--assume-timezone=Asia/Tokyo", "-o", "json"))
	if assumed.Rows()[0].Schedule.Timezone.Source != "assumed" || assumed.Rows()[0].Schedule.Next.Time == nil {
		t.Fatal("explicit assumption lost")
	}
	problems := decodeReport(t, s.cli("--problems", "-o", "json"))
	if len(problems.Rows()) != 2 || problems.Summary.Total != 2 {
		t.Fatal("Findings filter changed Summary scope")
	}
	quiet := decodeReport(t, s.cli("--no-findings", "-o", "json"))
	if quiet.HasFindings() || quiet.Rows()[0].Schedule.Expression == "" {
		t.Fatal("Finding suppression changed facts")
	}
	summary := decodeReport(t, s.cli("--summary=only", "-o", "json"))
	if summary.CronJobs != nil || summary.Summary.Total != 15 {
		t.Fatal("Summary projection failed")
	}
	if got := s.cli("--summary=only", "--exit-on-findings", "-o", "json"); got.Code != 2 {
		t.Fatalf("Finding exit: %+v", got)
	}
	for _, args := range [][]string{{"success", "-A"}, {"--no-findings", "--problems"}, {"--no-findings", "--exit-on-findings"}, {"--sort=bogus"}} {
		if got := s.cli(args...); got.Code != 1 || got.Out != "" {
			t.Fatalf("invalid flags accepted: %+v", got)
		}
	}
}

func (s *suite) inspect(t *testing.T) {
	r := decodeReport(t, s.cli("inspect", "manual-only", "-l", "e2e=primary", "-o", "json"))
	v := r.Inspection
	if r.Kind != "CronJobInspection" || v == nil || v.RetainedRuns == nil || *v.RetainedRuns != 3 || len(v.Runs) != 3 {
		t.Fatalf("owned manual Jobs or CronJob selector lost: %s", s.cli("inspect", "manual-only", "-o", "json").Out)
	}
	for _, run := range v.Runs {
		if run.Origin != "manual" || run.Slot.Time != nil || strings.Contains(run.Name, "unowned") {
			t.Fatalf("manual attribution: %+v", run)
		}
	}
	for _, format := range []string{"table", "wide", "yaml"} {
		r := s.must(s.cli("inspect", "manual-only", "-o", format, "--color=never"))
		if format == "yaml" {
			var report model.Report
			if err := yaml.Unmarshal([]byte(r.Out), &report); err != nil || report.Inspection == nil {
				t.Fatalf("inspection YAML: %v", err)
			}
		} else if !strings.Contains(r.Out, "Retained Jobs") {
			t.Fatal("missing detailed view")
		}
	}
	limited := decodeReport(t, s.cli("inspect", "manual-only", "--limit=1", "-o", "json"))
	if !limited.Inspection.RunsTruncated || *limited.Inspection.RetainedRuns != 3 || len(limited.Inspection.Runs) != 1 {
		t.Fatal("inspection limit changed retained evidence count")
	}
	reused := decodeReport(t, s.cli("inspect", "reused", "-o", "json"))
	s.must(s.kn("get", "job", "reused-old-owner"))
	if *reused.Inspection.RetainedRuns != 0 {
		t.Fatal("reused name claimed an old UID's Job")
	}
	retry := decodeReport(t, s.cli("inspect", "retry-pods", "-o", "json"))
	if len(retry.Inspection.Runs) != 1 || retry.Inspection.Runs[0].State != "complete" || retry.Inspection.Runs[0].FailedPods < 1 {
		t.Fatal("successful Job with failed retry Pod was misclassified")
	}
	scheduled := decodeReport(t, s.cli("inspect", "success", "-o", "json"))
	found := false
	for _, run := range scheduled.Inspection.Runs {
		if run.Origin != "scheduled" || run.Slot.Time == nil {
			continue
		}
		raw := s.job(run.Name)
		if run.SchedulingDelay.Seconds == nil || *run.SchedulingDelay.Seconds != raw.CreationTimestamp.Sub(*run.Slot.Time).Seconds() {
			t.Fatal("recorded scheduling delay is wrong")
		}
		if run.State == "complete" {
			found = true
			if run.ControllerStart.Time == nil || run.Finished.Time == nil || run.Duration.Seconds == nil || *run.Duration.Seconds != run.Finished.Time.Sub(*run.ControllerStart.Time).Seconds() {
				t.Fatal("recorded duration is wrong")
			}
		}
	}
	if !found {
		t.Fatal("no naturally scheduled completed Run inspected")
	}
	failed := decodeReport(t, s.cli("inspect", "failure", "-o", "json"))
	terminal := false
	for _, run := range failed.Inspection.Runs {
		if run.State == "failed" {
			terminal = true
			if run.Finished.Time == nil || run.FinishSource != "terminal-condition" || run.Duration.Seconds == nil {
				t.Fatal("failed Job duration unavailable despite real terminal condition")
			}
		}
	}
	if !terminal {
		t.Fatal("failed Job not inspected")
	}
	for _, source := range failed.Inspection.Evidence {
		if source.State != "available" {
			t.Fatalf("admin evidence read failed: %+v", source)
		}
	}
	quiet := decodeReport(t, s.cli("inspect", "failure", "--no-findings", "-o", "json"))
	if quiet.HasFindings() || len(quiet.Inspection.Runs) == 0 {
		t.Fatal("Finding suppression removed evidence")
	}
	if got := s.cli("inspect", "success", "-f", "/dev/null"); got.Code != 1 || got.Out != "" {
		t.Fatal("offline source hydrated into inspection")
	}
}

func (s *suite) runs(t *testing.T) {
	r := decodeReport(t, s.cli("runs", "manual-only", "--since=1h", "--limit=1000", "-o", "json"))
	h := r.History
	if r.Kind != "CronJobRunHistory" || h == nil || h.RetainedRuns == nil || *h.RetainedRuns != 3 || h.ProjectionBasis != "current-spec-only" {
		t.Fatal("history model or observed count lost")
	}
	observed, unknown := 0, 0
	for i, e := range h.Entries {
		if i > 0 && e.At.Time != nil && h.Entries[i-1].At.Time != nil && e.At.Time.After(*h.Entries[i-1].At.Time) {
			t.Fatal("history not newest first")
		}
		if e.Run != nil {
			observed++
			if e.Run.Origin != "manual" || e.Basis != "job-creation" || e.Run.Slot.Time != nil {
				t.Fatal("owned manual origin/clock lost")
			}
		} else {
			unknown++
			if e.State != "unknown" || e.Basis != "current-spec-projection" {
				t.Fatal("a missing Job was classified as a proven skip/deletion")
			}
		}
	}
	if observed != 3 || unknown < 1 {
		t.Fatal("observations and unknown projected slots were not both shown")
	}
	limited := decodeReport(t, s.cli("runs", "manual-only", "--limit=1", "-o", "json"))
	if !limited.History.EntriesTruncated || len(limited.History.Entries) != 1 || *limited.History.RetainedRuns != 3 {
		t.Fatal("history limit changed observed total")
	}
	window := decodeReport(t, s.cli("runs", "manual-only", "--since=1ns", "-o", "json"))
	if len(window.History.Entries) != 0 || *window.History.RetainedRuns != 0 {
		t.Fatal("history window included older runs")
	}
	unknownZone := decodeReport(t, s.cli("runs", "timezone-unknown", "-o", "json"))
	if unknownZone.History.ProjectionUnavailable != "controller-timezone-unknown" || len(unknownZone.History.Entries) == 0 {
		t.Fatal("unknown timezone hid observed Jobs or guessed a past phase")
	}
	retry := decodeReport(t, s.cli("runs", "retry-pods", "--limit=1000", "-o", "json"))
	found := false
	for _, e := range retry.History.Entries {
		if e.Run != nil {
			found = true
			if e.State != "complete" || e.Run.FailedPods < 1 {
				t.Fatal("failed retry Pod became a terminal Job failure")
			}
		}
	}
	if !found {
		t.Fatal("retry Job missing")
	}
	for _, format := range []string{"table", "wide", "yaml"} {
		r := s.must(s.cli("runs", "manual-only", "-o", format))
		if format == "yaml" {
			var report model.Report
			if err := yaml.Unmarshal([]byte(r.Out), &report); err != nil || report.History == nil {
				t.Fatalf("history YAML: %v", err)
			}
		}
	}
	for _, args := range [][]string{{"--since=0"}, {"--since=8761h"}, {"--limit=0"}, {"--limit=1001"}} {
		if got := s.cli(append([]string{"runs", "success"}, args...)...); got.Code != 1 || got.Out != "" {
			t.Fatal("invalid history limits accepted")
		}
	}
}

type runningCLI struct {
	cmd        *exec.Cmd
	lines      chan string
	done       chan error
	out, notes bytes.Buffer
	cancel     context.CancelFunc
}

func (s *suite) start(t *testing.T, args ...string) *runningCLI {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	p := &runningCLI{lines: make(chan string, 256), done: make(chan error, 1), cancel: cancel}
	p.cmd = exec.CommandContext(ctx, s.binary, append([]string{"--kubeconfig", s.kubeconfig, "--context", s.context}, args...)...)
	pipe, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = &p.notes
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(pipe)
		scanner.Buffer(make([]byte, 4096), 4*1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			p.out.WriteString(line + "\n")
			select {
			case p.lines <- line:
			default:
			}
		}
		err := p.cmd.Wait()
		if scanner.Err() != nil && err == nil {
			err = scanner.Err()
		}
		p.done <- err
	}()
	t.Cleanup(cancel)
	return p
}
func (p *runningCLI) line(t *testing.T, match func(string) bool) string {
	t.Helper()
	return p.lineWithin(t, 15*time.Second, match)
}
func (p *runningCLI) lineWithin(t *testing.T, limit time.Duration, match func(string) bool) string {
	t.Helper()
	timer := time.NewTimer(limit)
	defer timer.Stop()
	for {
		select {
		case line := <-p.lines:
			if match(line) {
				return line
			}
		case err := <-p.done:
			t.Fatalf("stream ended early: %v %s", err, p.notes.String())
		case <-timer.C:
			t.Fatal("stream did not produce expected output")
		}
	}
}
func (s *suite) stop(t *testing.T, p *runningCLI) {
	t.Helper()
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-p.done:
		if err != nil {
			t.Fatalf("Ctrl-C did not shut down cleanly: %v %s", err, p.notes.String())
		}
	case <-time.After(5 * time.Second):
		p.cancel()
		t.Fatal("stream leaked after Ctrl-C")
	}
	id := s.sequence.Add(1)
	prefix := filepath.Join(s.artifacts, fmt.Sprintf("%04d-stream", id))
	os.WriteFile(prefix+".stdout", p.out.Bytes(), 0600)
	os.WriteFile(prefix+".stderr", p.notes.Bytes(), 0600)
}

func (s *suite) logs(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "sidecar-manual-new"}, {[]string{"--run-offset=1"}, "sidecar-manual-failed"}, {[]string{"--failed", "-c", "worker"}, "worker-manual-failed"},
		{[]string{"--run=manual-old", "-c", "setup"}, "init-manual-old"}, {[]string{"--run=manual-new", "-c", "worker", "--since=1h"}, "worker-manual-new"},
	} {
		r := s.must(s.cli(append([]string{"logs", "manual-only"}, tc.args...)...))
		if !strings.Contains(r.Out, tc.want) {
			t.Fatalf("wrong Job/container logs: want %q in %s", tc.want, r.Out)
		}
	}
	r := s.must(s.cli("logs", "manual-only", "--all", "--all-containers", "--max-log-requests=1"))
	for _, job := range []string{"manual-old", "manual-new", "manual-failed"} {
		for _, container := range []string{"worker", "setup", "sidecar"} {
			if !strings.Contains(r.Out, "/"+job+"/") || !strings.Contains(r.Out, "/"+container+"]") {
				t.Fatalf("missing prefixed stream %s/%s", job, container)
			}
		}
	}
	tail := s.must(s.cli("logs", "manual-only", "--run=manual-new", "-c", "worker", "--tail=2", "--timestamps"))
	lines := strings.Split(strings.TrimSpace(tail.Out), "\n")
	if len(lines) != 2 {
		t.Fatalf("tail ignored: %s", tail.Out)
	}
	for _, line := range lines {
		if _, err := time.Parse(time.RFC3339Nano, strings.Fields(line)[0]); err != nil {
			t.Fatalf("missing API log timestamp: %s", line)
		}
	}
	current := s.must(s.cli("logs", "restart-container"))
	previous := s.must(s.cli("logs", "restart-container", "--previous"))
	if !strings.Contains(current.Out, "recovered-container-attempt") || !strings.Contains(previous.Out, "previous-container-attempt") || strings.Contains(previous.Out, "recovered-container-attempt") {
		t.Fatal("--previous selected another Job instead of the previous container")
	}
	retries := s.must(s.cli("logs", "retry-pods", "--all"))
	if !strings.Contains(retries.Out, "-fail") || !strings.Contains(retries.Out, "-success") || !strings.Contains(retries.Out, "["+s.ns+"/retry-success/") {
		t.Fatal("retry Pods not both read/prefixed")
	}
	for _, args := range [][]string{{"logs", "retry-pods", "--failed"}, {"logs", "manual-only", "-c", "missing"}, {"logs", "manual-only", "--run-offset=99"}, {"logs", "manual-only", "--all", "--all-containers", "-f", "--max-log-requests=1"}, {"logs", "manual-only", "--tail=-2"}} {
		if got := s.cli(args...); got.Code != 1 || got.Out != "" {
			t.Fatalf("invalid log selection accepted: %+v", got)
		}
	}
	p := s.start(t, "logs", "slow", "--run=slow-active", "-f", "--tail=1", "--prefix")
	p.line(t, func(line string) bool {
		return strings.Contains(line, "/slow-active/") && strings.Contains(line, "tick-")
	})
	s.stop(t, p)
	// Without --run, follow selects the latest active Job.
	active := s.start(t, "logs", "slow", "-f", "--tail=1")
	active.line(t, func(line string) bool { return strings.Contains(line, "tick-") })
	s.stop(t, active)
	var pods corev1.PodList
	json.Unmarshal([]byte(s.must(s.kn("get", "pods", "-l", "batch.kubernetes.io/job-name=slow-active", "-o", "json")).Out), &pods)
	if len(pods.Items) != 1 {
		t.Fatal("active fixture Pod missing")
	}
	patch, _ := json.Marshal(map[string]any{"spec": map[string]any{"ephemeralContainers": []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "e2e-debug", Image: s.image, ImagePullPolicy: corev1.PullNever, Command: []string{"sh", "-c", "echo ephemeral-ready; sleep 600"}}, TargetContainerName: "worker"}}}})
	if len(pods.Items[0].Spec.EphemeralContainers) == 0 {
		s.must(s.kn("patch", "pod", pods.Items[0].Name, "--subresource=ephemeralcontainers", "--type=merge", "-p", string(patch)))
	}
	s.wait("ephemeral container logs", 45*time.Second, func() bool {
		return strings.Contains(s.cli("logs", "slow", "--run=slow-active", "-c", "e2e-debug").Out, "ephemeral-ready")
	})
	s.createRun("manual-only", "manual-missing")
	s.terminal("manual-missing", batchv1.JobComplete)
	s.must(s.kn("delete", "pod", "-l", "batch.kubernetes.io/job-name=manual-missing", "--wait=true", "--timeout=30s"))
	missing := s.cli("logs", "manual-only", "--run=manual-missing")
	if missing.Code != 1 || missing.Out != "" || !strings.Contains(missing.Err, "cause is not recorded") {
		t.Fatalf("missing Pod evidence was guessed: %+v", missing)
	}
	s.must(s.kn("delete", "job", "manual-missing", "--wait=true", "--timeout=30s"))
}

func (s *suite) diagnose(t *testing.T) {
	s.terminal("long-observed", batchv1.JobComplete)
	s.wait("controller overlap Event", 90*time.Second, func() bool {
		var events corev1.EventList
		json.Unmarshal([]byte(s.kn("get", "events", "--field-selector", "reason=JobAlreadyActive", "-o", "json").Out), &events)
		return len(events.Items) > 0
	})
	s.wait("quota FailedCreate Event", 90*time.Second, func() bool {
		var events corev1.EventList
		json.Unmarshal([]byte(s.k("-n", s.quota, "get", "events", "--field-selector", "reason=FailedCreate", "-o", "json").Out), &events)
		return len(events.Items) > 0
	})
	r := decodeReport(t, s.cli("diagnose", "-A", "-o", "json"))
	if r.Kind != "CronJobDiagnostics" || r.Diagnostics == nil || len(*r.Diagnostics) != 17 {
		t.Fatal("diagnostic scope lost")
	}
	byName := map[string]map[string]bool{}
	for _, row := range *r.Diagnostics {
		rules := map[string]bool{}
		for _, f := range row.Findings {
			rules[f.Rule] = true
		}
		byName[row.Name] = rules
	}
	for name, rule := range map[string]string{"failure": "retained-job-failures", "allow-two": "concurrent-active-jobs", "long-complete": "duration-exceeds-current-gap", "external": "external-job-controller", "slow": "job-already-active-event", "blocked": "controller-warning-event", "pruned": "failed-history-disabled", "timezone-unknown": "timezone-not-set", "impossible": "no-next-occurrence"} {
		if !byName[name][rule] {
			t.Fatalf("real evidence did not produce %s for %s", rule, name)
		}
	}
	if byName["retry-pods"]["retained-job-failures"] {
		t.Fatal("successful retry Job reported as a failure")
	}
	for _, format := range []string{"table", "wide", "yaml"} {
		result := s.must(s.cli("diagnose", "failure", "-o", format))
		if format == "yaml" {
			var report model.Report
			if err := yaml.Unmarshal([]byte(result.Out), &report); err != nil || report.Diagnostics == nil {
				t.Fatalf("diagnostics YAML: %v", err)
			}
		} else if !strings.Contains(result.Out, "retained-job-failures") {
			t.Fatal("human runtime Finding missing")
		}
	}
	quiet := decodeReport(t, s.cli("diagnose", "-A", "--no-findings", "-o", "json"))
	if quiet.HasFindings() || len(*quiet.Diagnostics) != 17 {
		t.Fatal("suppression changed diagnostic scope")
	}
	if got := s.cli("diagnose", "failure", "--exit-on-findings", "-o", "json"); got.Code != 2 || got.Out == "" {
		t.Fatal("diagnostic exit status lost")
	}
	if got := s.cli("diagnose", "reused"); got.Code != 0 || got.Out != "" || !strings.Contains(got.Err, "remains unknown") {
		t.Fatal("empty Findings asserted health")
	}
}
