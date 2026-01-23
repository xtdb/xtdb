---
name: commit
description: Create a commit with contextual body explaining the why
user-invocable: true
disable-model-invocation: true
---

# Contextual Git Commit

The user will provide the commit headline as an argument to this command (e.g., `/commit fix: boolean logic error in expression.clj`).

## Your Responsibilities

1. **Check if anything is staged**:
   - Run `git diff --cached --quiet` to check if there are staged changes
   - If nothing is staged, inform the user and ask if they want to stage changes before committing
   - Stop and wait for the user to stage changes before proceeding

2. **Review the conversation history** to understand:
   - Why this change was made
   - What decisions were made and why
   - What tradeoffs were considered
   - Any counter-intuitive choices that need explanation
   - Context that would help future developers understand the reasoning

3. **Draft a commit body** that focuses on the **why**, not the what:
   - Explain the reasoning and context behind the change
   - Document any non-obvious decisions or tradeoffs
   - Include anything counter-intuitive that a developer familiar with the project would need to know
   - Omit obvious details that are self-evident from the diff
   - Keep it concise but informative

4. **Ask clarifying questions** if you're uncertain about:
   - The reasoning behind a decision
   - Whether something was a deliberate choice vs. a constraint
   - Any context that would be valuable for future developers

5. **Make the commit** directly with the commit body you've drafted:
   - Use the user's provided headline as the first line
   - Add a blank line
   - Add your drafted commit body
   - Commit what's already staged (do NOT stage or unstage anything)
   - The user will review the commit message in the Bash tool request before approving

## Important Constraints

- **DO NOT** stage or unstage any files - only commit what's already in the staging area
- **DO NOT** just describe what changed - focus on why it was done this way
- **DO** include a co-author header (replacing the model as appropriate):
  ```
  Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
  ```
- **DO NOT** push the commit

## Workflow

1. Check if there are any staged changes (run `git diff --cached --quiet` or `git diff --cached --name-only`)
2. If nothing is staged, inform the user and stop (do not proceed with the commit)
3. Parse the commit headline from the command arguments
4. Review the session to extract salient context
5. Draft the commit body
6. Ask any clarifying questions if needed
7. Make the commit with git commit (the user will review the commit message in the Bash tool request)
