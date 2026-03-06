"""Generate _logo_data.py with embedded logo as base64."""
import base64

with open("LogoOficial_Branco.png", "rb") as f:
    b64 = base64.b64encode(f.read()).decode("ascii")

with open("_logo_data.py", "w") as out:
    out.write("# Auto-generated: embedded logo as base64\n")
    out.write("LOGO_BASE64 = (\n")
    for i in range(0, len(b64), 100):
        chunk = b64[i:i+100]
        out.write(f'    "{chunk}"\n')
    out.write(")\n")

print("_logo_data.py created")
