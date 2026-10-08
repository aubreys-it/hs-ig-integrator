# =============================================================================
# FILE: __init__.py
# REPO PATH: hs-ig-integrator/pull-hs-payroll-zip/__init__.py
# (OVERWRITES the existing file. Only change: matches the payroll zip by its
#  exact name, HS_data_export_[yyyyMMdd]_0115.zip, instead of any
#  HS_data_export_[yyyyMMdd]_*.zip - HS also drops a daily _0100.zip.)
# =============================================================================

import azure.functions as func
import logging
import os
import re
import json
import tempfile
import pysftp
from paramiko.hostkeys import HostKeys, HostKeyEntry
from paramiko.ssh_exception import SSHException
from datetime import datetime
from ..modules import hsglob as g

# Suffix of the HS payroll export zip: HS_data_export_[yyyyMMdd]_0115.zip
PAYROLL_ZIP_SUFFIX = '_0115.zip'

# This function pulls the HotSchedules payroll export zip from the HS SFTP
# server (/datastore/Export/HS_data_export_[yyyyMMdd]_0115.zip) and lands it
# unmodified on aubdatain at hot-schedules/clock-data/zip/. Unzipping and
# per-location distribution happens in unpack-hs-payroll-zip.
#
# HS drops more than one HS_data_export_[yyyyMMdd]_*.zip per day (e.g. a daily
# _0100.zip). Only the _0115 zip is the payroll export, so the file is matched
# by exact name. If HS ever changes that suffix, this returns 404 - update
# PAYROLL_ZIP_SUFFIX above.
#
# File mechanics only - no SQL. ADF owns the dim.dates.isPayrollMonday check,
# the pr.fn_get_active_run_id lookup, and all pr.pipeline_audit writes, using
# this function's JSON response.
#
# Expects a JSON body: {"file_date": "yyyyMMdd"}
#
# Host keys are pinned: FTP_HOSTKEY and AUBDATAIN_SFTP_HOSTKEY each hold one
# or more known_hosts-format lines (e.g. "host ecdsa-sha2-nistp256 AAAA..."),
# separated by newlines or ";". List the server's current key and, when the
# vendor publishes one, its next key - a server presenting any listed key is
# accepted, so a planned rotation doesn't break the run. Only the key part of
# each line is used; it is matched against FTP_HOST / AUBDATAIN_SFTP_HOST.
# The function refuses to connect if a setting is missing or the server
# presents a key that isn't listed. paramiko checks the host key before it
# sends the password, so a non-matching server never sees the credentials.


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

    logging.info(f'pull-hs-payroll-zip starting for file_date={file_date_str}')

    expected_name = f'HS_data_export_{file_date_str}{PAYROLL_ZIP_SUFFIX}'
    matched_filename = None
    temp_file_path = None

    try:
        hs_keys = _pinned_keys(g.ftp_hostkey, 'FTP_HOSTKEY')
        aub_keys = _pinned_keys(g.aubdatain_sftp_hostkey, 'AUBDATAIN_SFTP_HOSTKEY')

        # Find and download the expected zip from the HS SFTP server
        with _connect(g.ftp_host, g.ftp_user, g.ftp_pass, hs_keys, 'FTP_HOSTKEY') as hs_sftp:
            remote_files = hs_sftp.listdir(g.hs_export_path)
            candidates = [f for f in remote_files if f == expected_name]

            if len(candidates) == 0:
                return _error_response(
                    f'No file named {expected_name} found in {g.hs_export_path} on the HS SFTP server.',
                    status_code=404
                )

            if len(candidates) > 1:
                return _error_response(
                    f'Multiple files named {expected_name} in {g.hs_export_path}: {candidates}'
                )

            matched_filename = candidates[0]
            remote_zip_path = f'{g.hs_export_path}/{matched_filename}'

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name

            hs_sftp.get(remote_zip_path, temp_file_path)
            logging.info(f'Downloaded {remote_zip_path} to {temp_file_path}')

        size_bytes = os.path.getsize(temp_file_path)

        # Push the zip to aubdatain, unmodified
        dest_path = f'{g.hs_zip_path}/{matched_filename}'
        with _connect(g.aubdatain_sftp_host, g.aubdatain_sftp_user, g.aubdatain_sftp_pass,
                      aub_keys, 'AUBDATAIN_SFTP_HOSTKEY') as dest_sftp:
            dest_sftp.put(temp_file_path, dest_path)
            logging.info(f'Uploaded {matched_filename} to aubdatain at {dest_path}')

    except Exception as e:
        return _error_response(f'Error pulling/landing HS payroll zip: {e}')

    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)

    return func.HttpResponse(
        json.dumps({
            'status': 'success',
            'file_date': file_date_str,
            'zip_filename': matched_filename,
            'dest_path': dest_path,
            'size_bytes': size_bytes
        }),
        status_code=200,
        mimetype='application/json'
    )