import clean
import pyinstaller

pyinstaller \
--clean \
  --noconfirm \
  --windowed \
  --name "Atlassian Commander by KIngram" \
  --icon assets/app.icns \
  --add-data "backup:backup" \
  --add-data "logs:logs" \
  --add-data "auth_profiles.json:." \
  main_updated.py
