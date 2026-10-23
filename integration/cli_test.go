// Copyright 2026 Mert Öztürk
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mertdotcc/kubectl-cron/internal/cli"
	"github.com/mertdotcc/kubectl-cron/internal/model"
)

// cronFile writes CronJob documents built from name, namespace, schedule, and
// timezone, each optionally followed by extra spec or status YAML.
func cronFile(t *testing.T, docs ...[5]string) string {
	t.Helper()
	var b strings.Builder
	for _, d := range docs {
		fmt.Fprintf(&b, "---\napiVersion: batch/v1\nkind: CronJob\nmetadata:\n  name: %s\n", d[0])
		if d[1] != "" {
			fmt.Fprintf(&b, "  namespace: %s\n", d[1])
		}
		fmt.Fprintf(&b, "spec:\n  schedule: %q\n", d[2])
		if d[3] != "" {
			fmt.Fprintf(&b, "  timeZone: %s\n", d[3])
		}
		b.WriteString(d[4])
	}
	path := filepath.Join(t.TempDir(), "cronjobs.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func report(t *testing.T, args ...string) model.Report {
	t.Helper()
	out, _, err := command("", args...)
	if err != nil && cli.ExitCode(err) != 2 {
		t.Fatalf("%v: %v", args, err)
	}
	var r model.Report
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		t.Fatalf("%v: %v\n%s", args, err, out)
	}
	return r
}

func names(r model.Report) []string {
	result := []string{}
	for _, row := range r.Rows() {
		result = append(result, row.Namespace+"/"+row.Name)
	}
	return result
}

func TestFileInputSourcesAndNamespaces(t *testing.T) {
	t.Setenv("KUBECONFIG", "/definitely/unavailable/config")
	path := cronFile(t, [5]string{"loose", "", "*/5 * * * *", "UTC"}, [5]string{"placed", "teams", "0 12 * * *", "UTC"})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out, _, err := command(string(data), "-f", "-", "-o", "json")
	if err != nil || !strings.Contains(out, `"name": "placed"`) {
		t.Fatalf("stdin input: %v\n%s", err, out)
	}
	if got := names(report(t, "-f", path, "-o", "json")); strings.Join(got, ",") != "/loose,teams/placed" {
		t.Fatalf("omitted namespace invented or declared one lost: %v", got)
	}
	if got := names(report(t, "-f", path, "-f", "../testdata/cronjobs.yaml", "-o", "json")); len(got) != 6 {
		t.Fatalf("repeated -f did not merge: %v", got)
	}
	single := cronFile(t, [5]string{"loose", "", "*/5 * * * *", "UTC"})
	if got := names(report(t, "-f", single, "-n", "filled", "-o", "json")); strings.Join(got, ",") != "filled/loose" {
		t.Fatalf("-n did not fill a missing namespace: %v", got)
	}
	for _, args := range [][]string{
		{"-f", path, "-n", "other"},
		{"-f", "https://example.com/cronjobs.yaml"},
		{"-f", filepath.Join(t.TempDir(), "missing.yaml")},
	} {
		out, _, err := command("", args...)
		if cli.ExitCode(err) != 1 || out != "" {
			t.Fatalf("%v accepted: %v %s", args, err, out)
		}
	}
}

func TestOverviewSortOrdersAndSummaryProjection(t *testing.T) {
	path := cronFile(t,
		[5]string{"zeta", "b", "0 9 * * *", "UTC", "status:\n  lastScheduleTime: \"2026-11-05T07:00:00Z\"\n"},
		[5]string{"omega", "a", "*/30 * * * *", "UTC", ""},
		[5]string{"alpha", "a", "0 10 * * *", "", "status:\n  lastScheduleTime: \"2026-11-04T10:00:00Z\"\n"},
		[5]string{"beta", "b", "0 0 31 2 *", "UTC", ""},
	)
	if got := strings.Join(names(report(t, "-f", path, "--sort=name", "-o", "json")), ","); got != "a/alpha,a/omega,b/beta,b/zeta" {
		t.Fatalf("name order: %s", got)
	}
	next := report(t, "-f", path, "--sort=next", "-o", "json").Rows()
	if next[0].Name != "omega" || next[1].Name != "zeta" || next[2].Schedule.Next.Time != nil || next[3].Schedule.Next.Time != nil {
		t.Fatalf("next order: %v", names(report(t, "-f", path, "--sort=next", "-o", "json")))
	}
	last := report(t, "-f", path, "--sort=last", "-o", "json").Rows()
	if last[0].LastSchedule.Time == nil || last[1].LastSchedule.Time == nil || last[2].LastSchedule.Time != nil {
		t.Fatal("recorded schedules do not sort before missing ones")
	}
	found := report(t, "-f", path, "--sort=findings", "-o", "json").Rows()
	if len(found[0].Findings) == 0 || len(found[1].Findings) == 0 || len(found[2].Findings) != 0 {
		t.Fatal("rows with Findings do not sort first")
	}
	hidden := report(t, "-f", path, "--summary=hide", "-o", "json")
	if hidden.Summary != nil || len(hidden.Rows()) != 4 {
		t.Fatal("--summary=hide kept the Summary or dropped rows")
	}
	shown := report(t, "-f", path, "-o", "json")
	if shown.Summary == nil || shown.Summary.Total != 4 || shown.Summary.TimezoneUnspecified != 1 {
		t.Fatalf("default Summary: %+v", shown.Summary)
	}
}

func TestInvalidArgumentsPrintNothing(t *testing.T) {
	t.Setenv("KUBECONFIG", "/definitely/unavailable/config")
	file := "../testdata/cronjobs.yaml"
	for _, args := range [][]string{
		{"-f", file, "--sort=bogus"},
		{"-f", file, "--summary=bogus"},
		{"-f", file, "-o", "bogus"},
		{"-f", file, "--color=bogus"},
		{"-f", file, "--watch-interval=1s"},
		{"-f", file, "--assume-timezone=No/Such_Zone"},
		{"-f", file, "--definitely-unknown-flag"},
		{"explain", "@hourly", "--count=0"},
		{"explain", "@hourly", "--count=1001"},
		{"explain", "@hourly", "-n", "finance"},
		{"explain", "@hourly", "-A"},
		{"explain", "@hourly", "-l", "team=ledger"},
		{"explain", "@hourly", "--assume-timezone=UTC"},
		{"inspect", "ledger", "-f", file},
		{"runs", "ledger", "-f", file},
		{"timeline", "ledger", "-f", file},
		{"completion", "bash"},
	} {
		out, _, err := command("", args...)
		if cli.ExitCode(err) != 1 || out != "" {
			t.Fatalf("%v accepted: %v %s", args, err, out)
		}
	}
}

func TestExplainReportsAndRelativeReferences(t *testing.T) {
	bare := report(t, "explain", "@every 1h", "--count=1000", "-o", "json")
	e := bare.Explanation
	if bare.Kind != "ScheduleExplanation" || bare.Source != "expression" || e == nil || len(e.Schedule.Occurrences) != 1000 || e.Schedule.AnchorSource != "evaluation" {
		t.Fatalf("bare relative Explanation: %+v", bare)
	}
	for expression, rule := range map[string]string{"@every 20s": "sub-minute-schedule", "CRON_TZ=UTC 0 * * * *": "embedded-timezone", "0 0 31 2 *": "no-next-occurrence"} {
		r := report(t, "explain", expression, "-o", "json")
		if len(r.Explanation.Findings) == 0 || r.Explanation.Findings[0].Rule != rule {
			t.Fatalf("%s: %+v", expression, r.Explanation.Findings)
		}
	}
	files := report(t, "explain", "-f", "../testdata/cronjobs.yaml", "--count=2", "-o", "json")
	if files.Source != "file" || len(files.Rows()) != 4 || files.Summary != nil {
		t.Fatalf("file Explanation: %+v", files)
	}
}

func TestFileFindingsAndReferencePoints(t *testing.T) {
	created := "  creationTimestamp: \"2026-11-01T00:00:00Z\"\n"
	path := cronFile(t,
		[5]string{"fast", "ops", "@every 30s", "UTC", ""},
		[5]string{"anchored", "ops", "@every 90m", "UTC", ""},
		[5]string{"floating", "ops", "@every 2h", "UTC", ""},
		[5]string{"martian", "ops", "0 * * * *", "Mars/Olympus_Mons", ""},
		[5]string{"embedded", "ops", "CRON_TZ=UTC 0 * * * *", "", ""},
	)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Give the first two documents a recorded creation time to anchor on.
	text := strings.Replace(string(data), "name: fast\n", "name: fast\n"+created, 1)
	text = strings.Replace(text, "name: anchored\n", "name: anchored\n"+created, 1)
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	rows := map[string]model.CronJob{}
	for _, row := range report(t, "-f", path, "-o", "json").Rows() {
		rows[row.Name] = row
	}
	rules := func(name string) string {
		ids := []string{}
		for _, f := range rows[name].Findings {
			ids = append(ids, f.Rule)
		}
		return strings.Join(ids, ",")
	}
	if !strings.Contains(rules("fast"), "sub-minute-schedule") {
		t.Fatalf("sub-minute: %s", rules("fast"))
	}
	if s := rows["anchored"].Schedule; s.AnchorSource != "creation" || s.Next.Time == nil {
		t.Fatalf("creation anchor: %+v", s)
	}
	if s := rows["floating"].Schedule; s.Next.Reason != "relative-anchor-unavailable" || s.Next.Time != nil {
		t.Fatalf("missing reference invented a phase: %+v", s)
	}
	if !strings.Contains(rules("martian"), "timezone-unavailable") || !strings.Contains(rules("embedded"), "embedded-timezone") {
		t.Fatalf("timezone Findings: %s / %s", rules("martian"), rules("embedded"))
	}
}

func TestAutoColorAndEveryCommandHelp(t *testing.T) {
	out, _, err := command("", "-f", "../testdata/cronjobs.yaml", "--color=auto")
	if err != nil || strings.Contains(out, "\x1b[") {
		t.Fatalf("auto color decorated a non-terminal: %v", err)
	}
	t.Setenv("NO_COLOR", "1")
	out, _, err = command("", "-f", "../testdata/cronjobs.yaml", "--color=always")
	if err != nil || !strings.Contains(out, "\x1b[") {
		t.Fatalf("--color=always yielded to NO_COLOR: %v", err)
	}
	for _, name := range []string{"explain", "inspect", "runs", "logs", "diagnose", "timeline", "calendar"} {
		out, _, err := command("", name, "--help")
		if err != nil || !strings.Contains(out, "kubectl cron "+name) {
			t.Fatalf("%s help: %v\n%s", name, err, out)
		}
	}
	for name, want := range map[string]bool{"inspect": true, "diagnose": true, "runs": false, "timeline": false} {
		out, _, _ := command("", name, "--help")
		if strings.Contains(out, "--require-evidence") != want {
			t.Fatalf("%s --require-evidence advertised=%t", name, !want)
		}
	}
}
