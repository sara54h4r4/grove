#!/usr/bin/env python3
"""Package reviewed Bluehost source. Never collect env files or completed config."""
import argparse
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[1]
FILES = {
    "hosting/bluehost/private/Store.php": ".grove/Store.php",
    "hosting/bluehost/private/schema.sql": ".grove/schema.sql",
    "hosting/bluehost/private/config.example.php": ".grove/config.example.php",
    "hosting/bluehost/public/index.php": "public_html/grove-store/index.php",
    "hosting/bluehost/public/.htaccess": "public_html/grove-store/.htaccess",
    "docs/coordination-bluehost.md": "GROVE-SETUP.md",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with zipfile.ZipFile(args.output, "x", compression=zipfile.ZIP_DEFLATED) as output:
        for directory, mode in ((".grove/", 0o40700), ("public_html/grove-store/", 0o40755)):
            info = zipfile.ZipInfo(directory)
            info.external_attr = (mode << 16) | 0x10
            output.writestr(info, b"")
        for source, target in FILES.items():
            info = zipfile.ZipInfo(target)
            info.external_attr = (0o100644 if target.startswith("public_html/") else 0o100600) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            output.writestr(info, (ROOT / source).read_bytes())
    print(f"Packaged {len(FILES)} reviewed source files: {args.output}")


if __name__ == "__main__":
    main()
