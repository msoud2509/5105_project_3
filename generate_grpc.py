#!/usr/bin/env python3
"""
Generate gRPC code from protobuf files and fix imports.
This script runs protoc and automatically converts absolute imports to relative imports.
"""

import subprocess
import re
import sys
from pathlib import Path

def generate_grpc():
    """Generate gRPC code from proto files."""
    cmd = [
        "python", "-m", "grpc_tools.protoc",
        "-IgRPC",
        "--python_out=gRPC",
        "--grpc_python_out=gRPC",
        "gRPC/mktplace.proto",
        "gRPC/service_node.proto"
    ]
    
    print("Generating gRPC code...")
    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    
    if result.returncode != 0:
        print(f"Error: protoc generation failed with code {result.returncode}")
        sys.exit(1)
    
    print("✅ gRPC code generated successfully")

def fix_imports():
    """Fix absolute imports to relative imports in generated files."""
    grpc_dir = Path(__file__).parent / "gRPC"
    
    # Files to fix: the *_pb2_grpc.py files
    grpc_files = [
        grpc_dir / "mktplace_pb2_grpc.py",
        grpc_dir / "service_node_pb2_grpc.py"
    ]
    
    print("\nFixing imports in generated files...")
    
    for filepath in grpc_files:
        if not filepath.exists():
            print(f"Warning: {filepath} not found")
            continue
        
        with open(filepath, 'r') as f:
            content = f.read()
        
        original_content = content
        
        # Replace absolute imports with relative imports
        # Match: import <module>_pb2 as <alias>
        content = re.sub(
            r'^import (\w+_pb2) as',
            r'from . import \1 as',
            content,
            flags=re.MULTILINE
        )
        
        if content != original_content:
            with open(filepath, 'w') as f:
                f.write(content)
            print(f"✅ Fixed imports in {filepath.name}")
        else:
            print(f"✅ {filepath.name} already has correct imports")

if __name__ == "__main__":
    generate_grpc()
    fix_imports()
    print("\n✅ All done! gRPC code is ready.")
