#!/usr/bin/env python3
"""
Convert markdown to DOCX, removing ASCII visualizations that don't render well in Word.
The ASCII visualizations are kept in the .md file for GitHub/Markdown viewers.
"""

import re
import subprocess
import sys
from pathlib import Path

def remove_ascii_blocks(markdown_content):
    """Remove code blocks that contain ASCII art (box-drawing characters)."""
    # Pattern to match code blocks
    code_block_pattern = r'```[^\n]*\n(.*?)```'

    def should_remove(match):
        content = match.group(1)
        # Check if block contains box-drawing characters (ASCII art)
        ascii_chars = '┌┐└┘│─═╔╗╚╝║▼►▲◄├┤┬┴┼'
        return any(c in content for c in ascii_chars)

    def replace_block(match):
        if should_remove(match):
            return ''  # Remove the entire block
        return match.group(0)  # Keep non-ASCII code blocks

    result = re.sub(code_block_pattern, replace_block, markdown_content, flags=re.DOTALL)

    # Clean up multiple blank lines left by removal
    result = re.sub(r'\n{3,}', '\n\n', result)

    return result

def convert_to_docx(input_md, output_docx):
    """Convert markdown to DOCX, removing ASCII art blocks."""
    # Read the markdown file
    with open(input_md, 'r', encoding='utf-8') as f:
        content = f.read()

    # Remove ASCII art blocks
    clean_content = remove_ascii_blocks(content)

    # Write to temporary file
    temp_md = Path(input_md).with_suffix('.temp.md')
    with open(temp_md, 'w', encoding='utf-8') as f:
        f.write(clean_content)

    # Convert using pandoc
    try:
        subprocess.run([
            'pandoc', str(temp_md),
            '-o', str(output_docx),
            '--from', 'markdown',
            '--to', 'docx'
        ], check=True)
        print(f"Converted {input_md} to {output_docx} (ASCII blocks removed for Word compatibility)")
    finally:
        # Clean up temp file
        temp_md.unlink()

if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else "BRD_Intranet_Search_Analytics.md"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "BRD_Intranet_Search_Analytics.docx"
    convert_to_docx(input_file, output_file)
