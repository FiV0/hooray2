# Slides: Worst-case optimal joins in Hooray

The slide deck for the talk on worst-case optimal joins in Hooray: a Datomic
recap, binary joins, the result set as a trie (depth first for Leapfrog
Triejoin, breadth first for Generic Join), the goal of one worst-case optimal
join over the whole query, the old `PrefixExtender` interface and the
`ExecPattern` + stages design that replaced it.

The deck is a claude.ai Slides artifact:
https://claude.ai/artifact/PW5o1ca3cGeCxt3D6JUofs (private).

## Files

`slides/project/` is a copy of the deck's own files, in the layout of the
claude.ai "Slides" artifact type:

```text
slides/project/
├── deck.json          # deck index: title, slide order, sections, fonts
└── slides/
    ├── cover.html     # one file per slide
    ├── datoms.html
    └── ...
```

Each slide file contains exactly one `<section>` on a fixed 1920×1080 canvas,
with inline styles only (no classes, `<style>` blocks or scripts). Speaker
notes are the `<aside>` at the end of each section. Opened directly in a
browser, the files are not a usable deck; the Slides viewer provides layout,
Present mode and PDF/PowerPoint export.

## Editing

The published deck is the source of truth. Edit it in the claude.ai slide
editor, or ask Claude Code to edit it with the Artifact tool. Footer page
numbers are plain text in each slide, so adding, removing or reordering
slides means renumbering the footers of the slides that moved.

## Syncing the local copy

After editing, ask Claude to pull the deck into this directory, for example:

> Pull the slides deck at <deck URL> into slides/project and commit it.

Claude reads `project/deck.json` and every `project/slides/*.html` file of the
published deck and replaces `slides/project/` with them, so slides removed from
the deck also disappear here.
