# Things about this project to keep in mind

## MCP Server Tools

The clojure-mcp server tools (mcp__clojure-mcp__*) should only be used for Clojure files (.clj, .cljs, .cljc). For other files like Kotlin (.kt) and Java (.java), use the standard tools (Read, Edit, Write, etc.).

## Git

Only commit and push when explicitly asked to by the user.
End Codex-created commits with `Co-authored-by: Codex <model> <noreply@openai.com>`.
Replace `<model>` with the model used to create the commit.
Don't ever open the default editor for creating commit messages. Always do
rebases non-interactively. If the command needs an editor always use `GIT_EDITOR=true`.

### Style

In Clojure, always prefer map destructuring instead of inline keyword lookup whenever there are more than two keys involved.
```clojure
;; not like this
(defn foo [item]
  (+ (:foo item) (:bar item)))

;; but like this
(defn for [{:keys [foo bar] :as _item}]
  (+ foo bar))
```
