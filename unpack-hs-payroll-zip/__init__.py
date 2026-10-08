# =============================================================================
# FILE: __init__.py
# REPO PATH: hs-ig-integrator/unpack-hs-payroll-zip/__init__.py
# (NEW function folder - create unpack-hs-payroll-zip/ and save this file in
#  it as __init__.py)
# =============================================================================

import azure.functions as func
import logging
import io
import re
import json
import zipfile
import pysftp
from paramiko.hostkeys import HostKeys, HostKeyEntry
from paramiko.ssh_exception import SSHException
from datetime import datetime
from ..modules import hsglob as g

# This function reads the zip landed by pull-hs-payroll-zip
# (aubdatain: hot-schedules/clock-data/zip/), extracts each location's
# HSPayrollStandardExport-[locId]-[timestamp]-[yyyyMMdd].csv, renames it to
# [yyyyMMdd][timestamp]_[locId2digit]_HSPayrollStandardExport.csv, and writes
# it to hot-schedules/clock-data/[locId2digit]/ (folder created if missing).
#
# File mechanics only - no SQL. The JSON response lists every location file
# as success / failed / empty (loc_id, file_name, row_count) so ADF can write
# the pr.pipeline_audit rows itself. Empty files (locations without WebClock)
# are expected and are not errors. Files in the zip that don't match the
# payroll CSV name pattern are listed under "skipped" for visibility.
#
# The automated HS feed has no header row, so row_count = non-blank lines.
#
# HTTP status: 200 = no failures, 207 = some files failed, 500 = nothing could
# be processed (zip missing/unreadable, no payroll CSVs, or every file failed).
#
# Expects a JSON body: {"file_date": "yyyyMMdd", "zip_filename": "..."}
# zip_filename is optional - if omitted, this looks for exactly one zip in
# the aubdatain zip folder matching HS_data_export_[file_date]_*.zip.
#
# Host keys are pinned the same way as pull-hs-payroll-zip: only
# AUBDATAIN_SFTP_HOSTKEY is used here (one or more known_hosts lines,
# separated by newlines or ";"). paramiko checks the host key before it sends
# the password, so a non-matching server never sees the credentials.

CSV_NAME_PATTERN = re.compile(r'HSPayrollStandardExport-(\d+)-(\d+)-(\d{8})\.csv$', re.IGNORECASE)


def _pinned_keys(value, setting):
    keys = []
    for line in re.split(r'[;\r\n]+', value or ''):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        entry = HostKeyEntry.from_line(line)
        if entry is None:
            raise ValueError(f'{setting} contains an entry that is not a valid known_hosts line.')
        keys.append(entry.key)
    if not keys:
        raise ValueError(f'{setting} app setting is missing or empty.')
    return keys


def _connect(host, username, password, keys, setting):
    # Try each pinned key until the server's key matches one of them.
    for key in keys:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = HostKeys()
        cnopts.hostkeys.add(host, key.get_name(), key)
        try:
            return pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts)
        except SSHException as e:
            # Wrong key, or the server doesn't offer this key type - try the next one
            if 'Bad host key' not in str(e) and 'no acceptable host key' not in str(e):
                raise
    raise SSHException(f'{host} presented a host key that is not listed in {setting}.')


def _error_response(message, status_code=500):
    logging.error(message)
    return func.HttpResponse(
        json.dumps({'status': 'error', 'message': message}),
        status_code=status_code,
        mimetype='application/json'
    )


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
    except ValueError:
        req_body = {}
    if not isinstance(req_body, dict):
        req_body = {}

    file_date_str = req_body.get('file_date')
    if not isinstance(file_date_str, str) or not re.fullmatch(r'\d{8}', file_date_str):
        return _error_response('Request body must include "file_date" as a yyyyMMdd string.', status_code=400)

    try:
        datetime.strptime(file_date_str, '%Y%m%d')
    except ValueError:
        return _error_response(f'Could not parse file_date "{file_date_str}" as yyyyMMdd.', status_code=400)

    zip_filename = req_body.get('zip_filename')
    if zip_filename is not None and (not isinstance(zip_filename, str)
                                     or not re.fullmatch(r'[\w.-]+\.zip', zip_filename)):
        return _error_response('"zip_filename", if given, must be a plain .zip file name (no folders).',
                               status_code=400)

    logging.info(f'unpack-hs-payroll-zip starting for file_date={file_date_str}')

    results = {'success': [], 'failed': [], 'empty': [], 'skipped': []}

    try:
        aub_keys = _pinned_keys(g.aubdatain_sftp_hostkey, 'AUBDATAIN_SFTP_HOSTKEY')

        with _connect(g.aubdatain_sftp_host, g.aubdatain_sftp_user, g.aubdatain_sftp_pass,
                      aub_keys, 'AUBDATAIN_SFTP_HOSTKEY') as sftp:

            if not zip_filename:
                expected_prefix = f'HS_data_export_{file_date_str}_'
                candidates = [f for f in sftp.listdir(g.hs_zip_path)
                              if f.startswith(expected_prefix) and f.endswith('.zip')]
                if len(candidates) != 1:
                    return _error_response(
                        f'Expected exactly one zip matching {expected_prefix}*.zip in {g.hs_zip_path}, '
                        f'found {len(candidates)}: {candidates}',
                        status_code=404 if not candidates else 500
                    )
                zip_filename = candidates[0]

            remote_zip_path = f'{g.hs_zip_path}/{zip_filename}'
            with sftp.open(remote_zip_path, 'rb') as remote_zip:
                zip_bytes = remote_zip.read()
            logging.info(f'Read {zip_filename} ({len(zip_bytes)} bytes) from {remote_zip_path}')

            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                for member in zf.namelist():
                    if member.endswith('/'):
                        continue  # folder entry
                    match = CSV_NAME_PATTERN.search(member)
                    if not match:
                        logging.info(f'Skipped non-payroll file in zip: {member}')
                        results['skipped'].append({'file_name': member})
                        continue

                    loc_id = int(match.group(1))
                    timestamp = match.group(2)
                    embedded_date = match.group(3)
                    loc_id_2digit = f'{loc_id:02d}'
                    new_filename = f'{embedded_date}{timestamp}_{loc_id_2digit}_HSPayrollStandardExport.csv'

                    try:
                        csv_bytes = zf.read(member)
                        row_count = sum(1 for line in csv_bytes.decode('utf-8', errors='replace').splitlines()
                                        if line.strip())

                        dest_dir = f'{g.hs_clock_data_path}/{loc_id_2digit}'
                        if not sftp.isdir(dest_dir):
                            sftp.mkdir(dest_dir)
                        dest_path = f'{dest_dir}/{new_filename}'
                        sftp.putfo(io.BytesIO(csv_bytes), dest_path)
                        logging.info(f'Uploaded {new_filename} to {dest_path} ({row_count} rows)')

                        file_result = {'loc_id': loc_id, 'file_name': new_filename, 'row_count': row_count}
                        results['empty' if row_count == 0 else 'success'].append(file_result)

                    except Exception as e:
                        logging.error(f'Failed to process {member} for loc_id {loc_id}: {e}')
                        results['failed'].append({'loc_id': loc_id, 'file_name': new_filename,
                                                  'row_count': None, 'message': str(e)})

    except Exception as e:
        return _error_response(f'Error reading/unpacking {zip_filename or "HS payroll zip"}: {e}')

    processed = len(results['success']) + len(results['empty'])
    if processed == 0 and not results['failed']:
        return _error_response(f'No HSPayrollStandardExport CSVs found in {zip_filename}.')

    if not results['failed']:
        status, status_code = 'success', 200
    elif processed:
        status, status_code = 'partial', 207
    else:
        status, status_code = 'failed', 500

    return func.HttpResponse(
        json.dumps({
            'status': status,
            'file_date': file_date_str,
            'zip_filename': zip_filename,
            'counts': {k: len(v) for k, v in results.items()},
            'results': results
        }),
        status_code=status_code,
        mimetype='application/json'
    )