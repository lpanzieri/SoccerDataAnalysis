import base64
import os

# Paste your base64 string here (replace this with your actual string)
BASE64_IMAGE = """
PASTE_YOUR_BASE64_STRING_HERE
"""

# Output path
output_dir = os.path.join(os.path.dirname(__file__), '..', 'images')
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, 'inter_milan_juventus_league_positions.png')

# Decode and write the image
with open(output_path, 'wb') as f:
    f.write(base64.b64decode(BASE64_IMAGE))

print(f"Image saved to {output_path}")
