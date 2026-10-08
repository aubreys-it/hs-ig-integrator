# =============================================================================
# FILE: __init__.py
# REPO PATH: hs-ig-integrator/modules/hsglob/__init__.py
# (OVERWRITES the existing file. Everything above the "New below" line is
#  unchanged; the lines below it are added for pull-hs-payroll-zip and
#  unpack-hs-payroll-zip.)
# =============================================================================

import os

uploads_url = os.environ.get("UPLOADS_URL")
company_id = 964328777

concepts = {
    5: 1590,
    8: 1590,
    9: 1590,
    12: 1590,
    26: 1590
}

ftp_host = os.environ.get('FTP_HOST')
ftp_user = os.environ.get('FTP_USER')
ftp_pass = os.environ.get('FTP_PASS')

# --- New below this line, for pull-hs-payroll-zip / unpack-hs-payroll-zip ---

# Folder on the HS SFTP server where the payroll export zip lands
hs_export_path = '/datastore/Export'

# aubdatain (ADLS Gen2, SFTP-enabled) - inbound landing zone
aubdatain_sftp_host = os.environ.get('AUBDATAIN_SFTP_HOST')
aubdatain_sftp_user = os.environ.get('AUBDATAIN_SFTP_USER')
aubdatain_sftp_pass = os.environ.get('AUBDATAIN_SFTP_PASS')

# Pinned SFTP host keys - one known_hosts-format line each
ftp_hostkey = os.environ.get('FTP_HOSTKEY')
aubdatain_sftp_hostkey = os.environ.get('AUBDATAIN_SFTP_HOSTKEY')

# SFTP paths on aubdatain (container/folder)
hs_zip_path = 'clock-data/zip'
hs_clock_data_path = 'clock-data'