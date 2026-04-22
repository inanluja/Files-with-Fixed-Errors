"""
master_pipeline.py
──────────────────
Runs the full daily bond report pipeline automatically.
Triggered by Windows Task Scheduler at 9:45 AM every weekday.

Logic:
  - If BOTH Excel files were modified today → run full pipeline → send emails
  - If not → skip silently (handles weekends and holidays automatically)
"""

import os
import json
import re
import shutil
import subprocess
import logging
import win32com.client as win32
from datetime import date, timedelta
from pathlib import Path


# ─── CONFIG — adjust these paths on your work PC ──────────────────────────────

# Folder where all .ipynb notebooks and arms_database.db live (network share)
NB_DIR = r'P:\Application\Risk Mgmt\MRM\Python Projects\ARMS'

# Local folder where the DB is copied during pipeline run (fast local disk)
# The pipeline reads/writes here, then copies the result back to NB_DIR at the end.
LOCAL_DIR   = r'C:\Users\AJ003230\ARMS_local'
LOCAL_DB    = os.path.join(LOCAL_DIR, 'arms_database.db')
NETWORK_DB  = os.path.join(NB_DIR,  'arms_database.db')

# All Excel files that must be updated today to trigger the pipeline
DATA_DIR         = r'P:\Application\Risk Mgmt\MRM\ARMS\arms_data_storage'
EXCEL_PORTFOLIO  = os.path.join(DATA_DIR, 'securities_portfolio.xlsm')
EXCEL_BLOOMBERG  = os.path.join(DATA_DIR, 'Source_Data_Bloom.xlsm')
EXCEL_OAS_EM     = os.path.join(DATA_DIR, 'Emustruuindex.xlsx')
EXCEL_OAS_GLOBAL = os.path.join(DATA_DIR, 'I04064US_index.xlsx')

# Log folder — pipeline writes a daily log here
LOG_DIR = r'P:\Application\Risk Mgmt\MRM\ARMS\logs'

# Your email — receives a notification if the pipeline crashes
ERROR_NOTIFY_EMAIL = 'OrkhanR.Vahabov@pashabank.az'

# ──────────────────────────────────────────────────────────────────────────────


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f'pipeline_{date.today()}.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  %(levelname)s  %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def get_previous_working_day():
    """Returns the most recent weekday before today (Mon→Fri, Tue→Mon, etc.)"""
    day = date.today() - timedelta(days=1)
    while day.weekday() >= 5:   # 5=Saturday, 6=Sunday
        day -= timedelta(days=1)
    return day.strftime('%Y-%m-%d')


def files_updated_today(*paths):
    """Returns True only if ALL given files were modified today."""
    today = date.today()
    for path in paths:
        if not os.path.exists(path):
            logging.warning(f'File not found: {path}')
            return False
        modified = date.fromtimestamp(os.path.getmtime(path))
        if modified != today:
            logging.info(f'Not updated today: {path}  (last modified: {modified})')
            return False
    return True


def set_valuation_date(nb_path, valuation_date):
    """
    Injects valuation_date into a notebook's first parameter cell.
    Finds the line:  valuation_date = 'YYYY-MM-DD'
    and replaces the date string.
    """
    with open(nb_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    replaced = False
    for cell in nb['cells']:
        if cell['cell_type'] != 'code':
            continue
        src = ''.join(cell['source'])
        if "valuation_date = '" in src:
            new_src = re.sub(
                r"valuation_date = '\d{4}-\d{2}-\d{2}'",
                f"valuation_date = '{valuation_date}'",
                src
            )
            cell['source'] = [new_src]
            replaced = True
            break   # only the first (params) cell

    if not replaced:
        logging.warning(f'valuation_date not found in {nb_path}')

    with open(nb_path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)


GREEN  = '\033[92m'
YELLOW = '\033[93m'
RED    = '\033[91m'
RESET  = '\033[0m'


def run_notebook(nb_name):
    """Executes a notebook in-place using nbconvert."""
    nb_path = os.path.join(NB_DIR, nb_name)
    print(f'  {YELLOW}► Running:{RESET} {nb_name}')
    logging.info(f'Running: {nb_name}')
    result = subprocess.run(
        [
            r'C:\Users\AJ003230\AppData\Local\anaconda3\Scripts\jupyter',
            'nbconvert',
            '--to', 'notebook',
            '--execute',
            '--inplace',
            '--ExecutePreprocessor.timeout=900',        # 15 min max per notebook
            '--ExecutePreprocessor.startup_timeout=120', # 2 min for kernel to start
            nb_path
        ],
        capture_output=True,
        text=True,
        cwd=LOCAL_DIR   # notebooks resolve relative 'arms_database.db' from here (local fast copy)
    )
    if result.returncode != 0:
        print(f'  {RED}✗ Failed:{RESET} {nb_name}')
        raise RuntimeError(
            f'{nb_name} failed.\n\nSTDERR:\n{result.stderr[-2000:]}'
        )
    print(f'  {GREEN}✓ Done:{RESET}    {nb_name}')
    logging.info(f'Done: {nb_name}')


def send_email(to, cc, subject, body, attachment_path):
    """Sends email via Outlook with one attachment."""
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = to
    mail.CC = cc
    mail.Subject = subject
    mail.Body = body
    if os.path.exists(attachment_path):
        mail.Attachments.Add(str(Path(attachment_path).resolve()))
        logging.info(f'Attachment added: {attachment_path}')
    else:
        logging.warning(f'Attachment not found, sending without file: {attachment_path}')
    mail.Send()
    logging.info(f'Email sent: {subject}')


def send_error_email(error_message):
    """Sends a crash notification to ERROR_NOTIFY_EMAIL."""
    try:
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = ERROR_NOTIFY_EMAIL
        mail.Subject = f'[PIPELINE ERROR] {date.today()}'
        mail.Body = f'The daily bond pipeline failed.\n\nError:\n{error_message}'
        mail.Send()
    except Exception as e:
        logging.error(f'Could not send error notification: {e}')


def get_stop_loss_path(valuation_date):
    """Builds the dynamic monthly folder path for the Stop Loss report."""
    vd = date.fromisoformat(valuation_date)
    month_folder = f'{vd.month}.{vd.strftime("%B")}'   # e.g. '4.April' (no leading zero)
    year_folder  = vd.strftime('%Y')
    filename     = f'Stop-Loss check {valuation_date}.xlsx'
    return os.path.join(
        r'P:\Application\Risk Mgmt\MRM\REPORTS (internal)\DAILY\Stop-Loss Monitoring',
        year_folder, month_folder, filename
    )


def get_bond_prices_path(valuation_date):
    return rf'P:\Application\Risk Mgmt\MRM\ARMS\Prices\prices_as_of {valuation_date}.xlsx'


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    setup_logging()
    logging.info('=' * 60)
    logging.info('Pipeline started')
    print(f'\n{GREEN}{"="*50}{RESET}')
    print(f'{GREEN}  ARMS Pipeline started — {date.today()}{RESET}')
    print(f'{GREEN}{"="*50}{RESET}\n')

    # ── Step 1: Check if all 4 Excel files were updated today ────────────────
    if not files_updated_today(EXCEL_PORTFOLIO, EXCEL_BLOOMBERG, EXCEL_OAS_EM, EXCEL_OAS_GLOBAL):
        logging.info('Not all Excel files updated today — pipeline skipped.')
        return

    logging.info('Both Excel files updated today — proceeding.')

    # ── Step 2: Determine valuation date ─────────────────────────────────────
    valuation_date = get_previous_working_day()
    logging.info(f'Valuation date: {valuation_date}')

    # ── Step 2b: Copy DB from network to local disk (fast reads/writes) ──────
    os.makedirs(LOCAL_DIR, exist_ok=True)
    print(f'  {YELLOW}► Copying DB to local...{RESET}')
    logging.info(f'Copying DB from network to local: {LOCAL_DB}')
    shutil.copy2(NETWORK_DB, LOCAL_DB)
    print(f'  {GREEN}✓ Local DB ready{RESET}\n')
    logging.info('Local DB ready.')

    try:
        # ── Step 3: Inject valuation_date into notebooks that use it ─────────
        # import_data_from_excel.ipynb has no valuation_date — skipped intentionally
        # Save_Oas_to_database.ipynb is NOT in the daily pipeline — run manually
        #   when Emustruuindex.xlsx / I04064US_index.xlsx are updated (weekly/monthly)
        notebooks_with_date = [
            'import_external_data.ipynb',
            'Metrics_calculation.ipynb',
            'Report_Bond_Prices.ipynb',
            'Report_Stop_Loss.ipynb',
        ]
        for nb in notebooks_with_date:
            set_valuation_date(os.path.join(NB_DIR, nb), valuation_date)
        logging.info('valuation_date injected into notebooks.')

        # ── Step 4: Import Excel files into DB ────────────────────────────────
        run_notebook('import_data_from_excel.ipynb')

        # ── Step 5: Import OAS data (EM + Global) ────────────────────────────
        run_notebook('Save_Oas_to_database.ipynb')

        # ── Step 6: Import CBAR FX rates and yield curves ────────────────────
        run_notebook('import_external_data.ipynb')

        # ── Step 6: Run metrics calculation ──────────────────────────────────
        run_notebook('Metrics_calculation.ipynb')

        # ── Step 6b: Copy updated DB back to network ─────────────────────────
        # Done right after metrics so reports below read the fresh data either
        # from local (fast) or network (consistent) — both are identical now.
        print(f'\n  {YELLOW}► Copying DB back to network...{RESET}')
        logging.info('Copying updated DB back to network...')
        shutil.copy2(LOCAL_DB, NETWORK_DB)
        print(f'  {GREEN}✓ Network DB updated{RESET}\n')
        logging.info('Network DB updated.')

        # ── Step 7: Generate Bond Prices report ──────────────────────────────
        run_notebook('Report_Bond_Prices.ipynb')

        # ── Step 8: Generate Stop Loss report ────────────────────────────────
        run_notebook('Report_Stop_Loss.ipynb')

        # ── Step 9: Send Bond Prices email ────────────────────────────────────
        send_email(
            to='accounting.tax@pashabank.az; treasury@pashabank.az; '
               'management.reporting@pashabank.az; noncash.operations@pashabank.az',
            cc='Mrm@pashabank.az; OrkhanR.Vahabov@pashabank.az',
            subject=f'Bond Prices {valuation_date}',
            body=(
                f'Dear colleagues,\n\n'
                f'Please find attached Bond Prices as of {valuation_date}.\n\n'
                f'Best Regards,\nMRM Team'
            ),
            attachment_path=get_bond_prices_path(valuation_date)
        )

        # ── Step 10: Send Stop Loss email ─────────────────────────────────────
        try:
            status_file = os.path.join(NB_DIR, 'stop_loss_status.txt')
            if os.path.exists(status_file):
                stop_loss_status = open(status_file, encoding='utf-8').read().strip() or 'See attached'
            else:
                stop_loss_status = 'See attached'
        except Exception:
            stop_loss_status = 'See attached'
        send_email(
            to='Nazir.Davudov@pashabank.az; Narmin.Ahmadzada@pashabank.az; '
               'Nijat.Piriyev@pashabank.az; HasanO.Aliyev@pashabank.az',
            cc='OrkhanR.Vahabov@pashabank.az; Kamil.Allahverdiyev@pashabank.az; '
               'Sanan.Mustafayev@pashabank.az; Sarkhan.Piriyev@pashabank.az; '
               'Amil.Majidov@pashabank.az; Mrm@pashabank.az',
            subject=f'Stop-loss_{valuation_date}',
            body=(
                f'Dear colleagues,\n\n'
                f'Please find attached result of stop-loss check as of {valuation_date}.\n\n'
                f'Status: {stop_loss_status}\n\n'
                f'Best Regards,\nMRM Team'
            ),
            attachment_path=get_stop_loss_path(valuation_date)
        )

        print(f'\n{GREEN}{"="*50}{RESET}')
        print(f'{GREEN}  ✓ Pipeline completed successfully!{RESET}')
        print(f'{GREEN}{"="*50}{RESET}\n')
        logging.info('Pipeline completed successfully.')

    except Exception as e:
        logging.error(f'Pipeline failed: {e}')
        send_error_email(str(e))


if __name__ == '__main__':
    main()
