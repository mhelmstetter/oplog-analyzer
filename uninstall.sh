#!/bin/bash

# Oplog Analyzer Uninstall Script

set -e

INSTALL_DIR="$HOME/.local/bin"
SHARE_DIR="$HOME/.local/share/oplog-analyzer"
SCRIPT_NAME="oplog-analyzer"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_color() {
    printf "${1}${2}${NC}\n"
}

print_color $BLUE "┌─────────────────────────────────────────────┐"
print_color $BLUE "│        Oplog Analyzer Uninstaller          │"
print_color $BLUE "└─────────────────────────────────────────────┘"
echo

print_color $YELLOW "This will remove oplog-analyzer from your system."
read -p "Are you sure you want to continue? [y/N]: " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_color $BLUE "Uninstall cancelled."
    exit 0
fi

print_color $BLUE "Uninstalling Oplog Analyzer..."

# Remove wrapper script
if [ -f "$INSTALL_DIR/$SCRIPT_NAME" ]; then
    rm -f "$INSTALL_DIR/$SCRIPT_NAME"
    print_color $GREEN "✓ Removed wrapper script"
else
    print_color $YELLOW "⚠ Wrapper script not found"
fi

# Remove share directory
if [ -d "$SHARE_DIR" ]; then
    rm -rf "$SHARE_DIR"
    print_color $GREEN "✓ Removed application files"
else
    print_color $YELLOW "⚠ Application directory not found"
fi

print_color $GREEN "✓ Oplog Analyzer has been uninstalled"
echo
print_color $BLUE "Note: Your PATH configuration has not been modified."
print_color $BLUE "If you added $INSTALL_DIR to your PATH for this tool,"
print_color $BLUE "you may want to keep it for other applications."