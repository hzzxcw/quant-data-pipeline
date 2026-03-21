#!/bin/bash
#===============================================================================
# quant-data-pipeline Worktree Management Script
# 
# Usage:
#   ./scripts/worktree.sh list                    # List all worktrees
#   ./scripts/worktree.sh issues                 # List open GitHub issues
#   ./scripts/worktree.sh start <issue_number>   # Create worktree for issue
#   ./scripts/worktree.sh done                    # Finish current worktree
#   ./scripts/worktree.sh switch <name>           # Switch to worktree
#===============================================================================

set -e

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKTREES_BASE="$(dirname "$REPO_DIR")"
GITHUB_REPO="hzzxcw/quant-data-pipeline"
CURRENT_BRANCH=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Get current branch if in a worktree
get_current_branch() {
    if git rev-parse --git-dir > /dev/null 2>&1; then
        git branch --show-current 2>/dev/null || echo ""
    fi
}

# List all worktrees
cmd_list() {
    log_info "Project: $REPO_DIR"
    echo ""
    log_info "Main worktree (develop):"
    cd "$REPO_DIR"
    echo "  Path: $REPO_DIR"
    echo "  Branch: $(git branch --show-current)"
    echo ""
    log_info "Feature worktrees:"
    
    for wt in $(git worktree list --porcelain | grep -B1 "^worktree" | grep -v "^worktree" | awk '{print $2}'); do
        if [ "$wt" != "$REPO_DIR" ]; then
            wt_name=$(basename "$wt")
            wt_branch=$(cd "$wt" && git branch --show-current 2>/dev/null || echo "detached")
            echo "  $wt_name"
            echo "    Path: $wt"
            echo "    Branch: $wt_branch"
            echo ""
        fi
    done
}

# List open GitHub issues
cmd_issues() {
    log_info "Open issues on $GITHUB_REPO:"
    echo ""
    gh issue list --state open --json number,title,labels --jq '
        .[] | "#\(.number) - \(.title)" + 
        (if (.labels | length) > 0 then " [" + (.labels | map(.name) | join(", ")) + "]" else "" end)
    ' | while read line; do
        echo "  $line"
    done
}

# Create worktree for an issue
cmd_start() {
    local issue_number="$1"
    
    if [ -z "$issue_number" ]; then
        log_error "Usage: $0 start <issue_number>"
        exit 1
    fi
    
    # Fetch issue info
    local issue_info=$(gh issue view "$issue_number" --json number,title,labels --jq '{number: .number, title: .title, labels: [.labels[].name]}')
    local issue_title=$(echo "$issue_info" | jq -r '.title')
    local issue_labels=$(echo "$issue_info" | jq -r '.labels | join("-")')
    
    # Sanitize title for branch name
    local branch_name="feature/issue-${issue_number}-$(echo "$issue_title" | sed 's/[^a-zA-Z0-9]/-/g' | tr '[:upper:]' '[:lower:]' | cut -c1-50)"
    local worktree_path="$WORKTREES_BASE/quant-data-pipeline-${branch_name#feature/}"
    
    # Check if worktree already exists
    if [ -d "$worktree_path" ]; then
        log_warn "Worktree already exists: $worktree_path"
        log_info "Switching to existing worktree..."
        cd "$worktree_path"
        log_success "Ready at: $worktree_path"
        log_info "Run: cd $worktree_path"
        return
    fi
    
    # Create worktree from develop branch
    log_info "Creating worktree for issue #$issue_number..."
    log_info "Branch: $branch_name"
    log_info "Path: $worktree_path"
    
    cd "$REPO_DIR"
    git worktree add -b "$branch_name" "$worktree_path" develop
    
    # Initialize the new worktree
    cd "$worktree_path"
    
    log_success "Worktree created successfully!"
    echo ""
    log_info "Issue: #$issue_number - $issue_title"
    log_info "Branch: $branch_name"
    echo ""
    log_info "Next steps:"
    echo "  1. cd $worktree_path"
    echo "  2. Implement the feature"
    echo "  3. Run tests: uv run pytest"
    echo "  4. When done: ../scripts/worktree.sh done"
}

# Finish current worktree (commit, push, PR)
cmd_done() {
    local worktree_path=$(pwd)
    local branch_name=$(git branch --show-current)
    
    if [ -z "$branch_name" ] || [[ "$branch_name" != feature/* ]]; then
        log_error "This command must be run from a feature worktree"
        exit 1
    fi
    
    log_info "Finishing feature: $branch_name"
    echo ""
    
    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD -- 2>/dev/null; then
        log_error "You have uncommitted changes. Please commit first."
        git status --short
        exit 1
    fi
    
    # Check commits
    local commits_ahead=$(git log --oneline develop..HEAD 2>/dev/null | wc -l | tr -d ' ')
    if [ "$commits_ahead" -eq 0 ]; then
        log_warn "No commits to push"
        return
    fi
    
    log_info "Pushing branch..."
    git push -u origin "$branch_name"
    
    log_info "Creating PR..."
    gh pr create \
        --base develop \
        --head "$branch_name" \
        --title "[Feature] $(echo "$branch_name" | sed 's/feature\/issue-[0-9]*-//' | tr '-' ' ' | sed 's/.*/\u&/')" \
        --body "## Summary
TODO: Describe the feature implemented.

## Changes
- TODO: List key changes

## Testing
- [ ] Tests pass
- [ ] Code reviewed

Closes #$(echo "$branch_name" | grep -oE '[0-9]+' | head -1)"
    
    log_success "PR created! Visit: $(gh pr view --json url --jq '.url')"
}

# Switch to worktree
cmd_switch() {
    local target="$1"
    
    if [ -z "$target" ]; then
        log_error "Usage: $0 switch <worktree_name>"
        exit 1
    fi
    
    local worktree_path="$WORKTREES_BASE/quant-data-pipeline-$target"
    
    if [ ! -d "$worktree_path" ]; then
        log_error "Worktree not found: $worktree_path"
        exit 1
    fi
    
    log_success "Switching to: $worktree_path"
    cd "$worktree_path"
    echo ""
    log_info "Run: cd $worktree_path"
}

# Show help
cmd_help() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  list                    List all worktrees"
    echo "  issues                  List open GitHub issues"
    echo "  start <issue_number>    Create worktree for an issue"
    echo "  done                    Finish current worktree (commit, push, PR)"
    echo "  switch <name>           Switch to a worktree"
    echo "  help                    Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 issues               # See what needs to be done"
    echo "  $0 start 42             # Start working on issue #42"
    echo "  cd ../quant-data-pipeline-feature/issue-42-xxx  # Go to worktree"
    echo "  # ... implement feature ..."
    echo "  $0 done                 # Finish and create PR"
}

# Main
case "${1:-help}" in
    list)   cmd_list ;;
    issues) cmd_issues ;;
    start)  cmd_start "$2" ;;
    done)   cmd_done ;;
    switch) cmd_switch "$2" ;;
    help|--help|-h) cmd_help ;;
    *)      log_error "Unknown command: $1"; cmd_help; exit 1 ;;
esac
