// Copyright 2026 Mert Öztürk
// SPDX-License-Identifier: Apache-2.0

package render

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// cell is one table cell: the text that decides how wide its column is, and
// the coloured form that is printed once the width is known.
//
// The two are kept apart because a terminal gives an escape sequence no width
// at all and tabwriter counts every byte of one, so a coloured cell measured
// as it prints pushes its column out by the length of its colour.
type cell struct {
	text string
	// colored is empty for a cell that prints exactly as it measures, which
	// is every cell when colour is disabled.
	colored string
}

// plain is a cell the terminal prints as it is.
func plain(text string) cell { return cell{text: text} }

// print is the cell as it goes to the terminal.
func (c cell) print() string {
	if c.colored == "" {
		return c.text
	}
	return c.colored
}

// table is a grid of cells written through tabwriter, so every column ends up
// as wide as its widest cell and no wider.
type table struct {
	rows [][]cell
}

func (t *table) add(cells ...cell) { t.rows = append(t.rows, cells) }

// Two spaces between columns, as kubectl's own tables have, and no minimum
// width, so a column of short cells does not take a wide one's room.
const (
	columnPadding  = 2
	columnMinWidth = 0
	columnTabWidth = 0
)

// write aligns the table and writes it out.
//
// Alignment is measured on the uncoloured text and the colour is put back
// afterwards, line by line: every cell sits at a known offset in the aligned
// line, because tabwriter pads a cell on the right and never rewrites it.
func (t *table) write(w io.Writer) error {
	var aligned bytes.Buffer
	tw := tabwriter.NewWriter(&aligned, columnMinWidth, columnTabWidth, columnPadding, ' ', 0)
	for _, row := range t.rows {
		for i, c := range row {
			if i > 0 {
				fmt.Fprint(tw, "\t")
			}
			fmt.Fprint(tw, c.text)
		}
		fmt.Fprintln(tw)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	lines := strings.SplitAfter(aligned.String(), "\n")
	for i, row := range t.rows {
		if i >= len(lines) {
			break
		}
		if _, err := io.WriteString(w, trimRight(colorize(lines[i], row))); err != nil {
			return err
		}
	}
	return nil
}

// trimRight drops the padding tabwriter puts after the last cell of a line,
// which is invisible on a terminal and noise in a golden file or a diff. The
// padding is plain spaces after everything the cells wrote, colour included,
// so nothing but padding is ever trimmed.
func trimRight(line string) string {
	return strings.TrimRight(line, " \t\n") + "\n"
}

// colorize puts each cell's colour back into an aligned line, leaving the
// padding between them untouched. A line that does not read back as the cells
// it was written from is printed as it is, because losing the colour is better
// than losing the alignment.
func colorize(line string, row []cell) string {
	var out strings.Builder
	pos := 0
	for _, c := range row {
		end := pos + len(c.text)
		if end > len(line) || line[pos:end] != c.text {
			return line
		}
		out.WriteString(c.print())
		pos = end
		for pos < len(line) && line[pos] == ' ' {
			out.WriteByte(' ')
			pos++
		}
	}
	out.WriteString(line[pos:])
	return out.String()
}
