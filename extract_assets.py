import sys
from pathlib import Path
import re

html_path = Path(r"c:\fast-flow\FFEngineCommunity\src\ffengine\ui\templates\etl_studio\index.html")
content = html_path.read_text("utf-8")

# Extract style
style_match = re.search(r"<style>(.*?)</style>", content, re.DOTALL)
if style_match:
    css_content = style_match.group(1).strip() + "\n"
    # New HTML
    content = re.sub(r"<style>.*?</style>", '<link rel="stylesheet" href="/etl-studio/static/etl_studio/css/style.css">', content, flags=re.DOTALL)
    
    # Write CSS
    css_path = Path(r"c:\fast-flow\FFEngineCommunity\src\ffengine\ui\static\etl_studio\css\style.css")
    css_path.parent.mkdir(parents=True, exist_ok=True)
    css_path.write_text(css_content, "utf-8")

# Extract script
script_match = re.search(r"<script>(.*?)</script>(?=\s*(</body>|</html>))", content, re.DOTALL)
if script_match:
    js_content = script_match.group(1).strip() + "\n"
    # New HTML
    content = re.sub(r"<script>(.*?)</script>(?=\s*(</body>|</html>))", '<script src="/etl-studio/static/etl_studio/js/app.js"></script>', content, flags=re.DOTALL)
    
    # Write JS
    js_path = Path(r"c:\fast-flow\FFEngineCommunity\src\ffengine\ui\static\etl_studio\js\app.js")
    js_path.parent.mkdir(parents=True, exist_ok=True)
    js_path.write_text(js_content, "utf-8")

# Overwrite HTML
html_path.write_text(content, "utf-8")
print("Extraction complete.")
