#!/usr/bin/env python
"""
Transform runner for ETL pipeline.
Usage: python run_transform.py <table> <input_csv> <output_csv>

Looks for transformation scripts under `staging_transformation/cleaners` or
`staging_transformation`. If a matching script/module exists it will be used.
Otherwise, a builtin transform using `backend/functions/functions.py` is applied
and the transformed CSV is written to the output path.
"""
import sys
import os
import shutil
import importlib.util
import subprocess
import csv
import json


def run_module_transform(module_path, inp, outp):
    spec = importlib.util.spec_from_file_location('transform_mod', module_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if hasattr(mod, 'transform_file'):
        mod.transform_file(inp, outp)
        return 0
    # no transform_file; try running as script
    proc = subprocess.run([sys.executable, module_path, inp, outp], capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
    return proc.returncode


def load_builtin_functions():
    # functions.py located at backend/functions/functions.py relative to this file
    base = os.path.dirname(__file__)
    func_path = os.path.normpath(os.path.join(base, '..', 'functions', 'functions.py'))
    if not os.path.exists(func_path):
        return None
    spec = importlib.util.spec_from_file_location('fw', func_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def apply_builtin_transform(fw, table, rows, table_cfg):
    transformed = []
    for r in rows:
        nr = {}
        # clean strings
        for k, v in r.items():
            val = v
            if fw and v is not None:
                try:
                    val = fw.clean_string_field(v)
                except Exception:
                    val = v
            nr[k] = val

        # airlines
        try:
            if fw and ('airline_code' in nr or 'airline_name' in nr):
                if 'airline_name' in nr and nr.get('airline_name'):
                    nr['airline_name'] = fw.standardize_airline_name(nr['airline_name'])
                if 'airline_code' in nr and nr.get('airline_code'):
                    ok = fw.validate_airline_code(nr['airline_code'])
                    if not ok:
                        nr['_airline_code_invalid'] = 'true'
                    nr['airline_code'] = nr['airline_code'].upper()
                if 'airline_code' in nr and 'airline_name' in nr and nr.get('airline_code') and nr.get('airline_name'):
                    nr['airline_hash'] = fw.hash_airline(nr['airline_code'], nr['airline_name'])
        except Exception:
            pass

        # passengers
        try:
            if fw and ('passenger_id' in nr or 'name' in nr or 'email' in nr):
                if 'name' in nr and nr.get('name'):
                    nr['name'] = fw.standardize_name(nr['name'])
                    first, last = fw.detect_name_format(nr['name'])
                    if first: nr['first_name'] = first
                    if last: nr['last_name'] = last
                nr = fw.mask_passenger_pii(nr)
                if 'passenger_id' in nr and nr.get('passenger_id'):
                    nr['_passenger_id_valid'] = str(fw.validate_passenger_id(nr['passenger_id']))
        except Exception:
            pass

        # booking-sales
        try:
            if fw and any(x in nr for x in ['base_fare', 'taxes', 'fees', 'total']):
                try:
                    base = float(nr.get('base_fare') or 0)
                    taxes = float(nr.get('taxes') or 0)
                    fees = float(nr.get('fees') or 0)
                    total = float(nr.get('total') or 0)
                    valid, msg = fw.validate_booking_amount(base, taxes, fees, total)
                    nr['_booking_amount_valid'] = str(valid)
                    if not valid:
                        nr['_booking_amount_msg'] = msg
                except Exception:
                    pass
                # currency conversion
                if 'currency' in nr and nr.get('currency'):
                    try:
                        amt = float(nr.get('total') or 0)
                        usd = fw.convert_currency(amt, nr.get('currency'), 'USD')
                        if usd is not None:
                            nr['amount_usd'] = '{:.2f}'.format(usd)
                    except Exception:
                        pass
        except Exception:
            pass

        # generic date normalization: fields containing 'date'
        try:
            for k in list(nr.keys()):
                if 'date' in k.lower() and nr.get(k):
                    ok_dt = None
                    if fw:
                        try:
                            ok, dt = fw.validate_date(nr[k])
                            if ok and dt:
                                nr[k] = dt.isoformat()
                        except Exception:
                            pass
        except Exception:
            pass

        transformed.append(nr)

    # deduplicate using natural key from config if available
    try:
        nat = None
        if table_cfg:
            nat = table_cfg.get('natural_key')
        if not nat:
            for cand in ['id', 'booking_reference', 'passenger_id', 'flight_key']:
                if cand in transformed[0].keys():
                    nat = cand
                    break
        if nat:
            seen = set()
            deduped = []
            for r in transformed:
                key = r.get(nat)
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(r)
            transformed = deduped
    except Exception:
        pass

    return transformed


def main():
    if len(sys.argv) < 4:
        print('Usage: run_transform.py <table> <input_csv> <output_csv>')
        sys.exit(2)
    table = sys.argv[1]
    inp = sys.argv[2]
    outp = sys.argv[3]

    base = os.path.dirname(__file__)
    trans_dirs = [os.path.join(base, 'staging_transformation', 'cleaners'), os.path.join(base, 'staging_transformation')]
    candidates = []
    for d in trans_dirs:
        if not os.path.isdir(d):
            continue
        for name in os.listdir(d):
            if not name.endswith('.py'):
                continue
            candidates.append(os.path.join(d, name))

    # Prefer module named staging_<table>.py or <table>_transform.py
    chosen = None
    for c in candidates:
        nm = os.path.basename(c)
        if nm == f'staging_{table}.py' or nm == f'{table}_transform.py' or nm == f'{table}.py':
            chosen = c
            break

    if not chosen and candidates:
        # try simple heuristic: any file that contains table name
        for c in candidates:
            if table in os.path.basename(c):
                chosen = c
                break

    if chosen:
        rc = run_module_transform(chosen, inp, outp)
        sys.exit(rc)

    # no external transform found: apply builtin transforms using functions.py if available
    fw = load_builtin_functions()
    if not fw:
        try:
            shutil.copyfile(inp, outp)
            sys.exit(0)
        except Exception as e:
            print('Copy failed:', e, file=sys.stderr)
            sys.exit(1)

    # load optional table configs to guide transforms
    table_cfg = {}
    try:
        cfg_path = os.path.join(os.path.dirname(__file__), 'table_configs.json')
        if os.path.exists(cfg_path):
            with open(cfg_path, 'r', encoding='utf8') as fh:
                table_cfg = json.load(fh)
    except Exception:
        table_cfg = {}

    # read CSV
    rows = []
    try:
        with open(inp, 'r', encoding='utf8') as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                rows.append(dict(r))
    except Exception as e:
        print('Failed reading input CSV:', e, file=sys.stderr)
        sys.exit(1)

    if not rows:
        # nothing to do, copy
        shutil.copyfile(inp, outp)
        sys.exit(0)

    transformed = apply_builtin_transform(fw, table, rows, table_cfg.get(table))

    # write out transformed CSV
    try:
        out_fields = set()
        for r in transformed:
            out_fields.update(r.keys())
        out_fields = list(out_fields)
        with open(outp, 'w', encoding='utf8', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=out_fields)
            writer.writeheader()
            for r in transformed:
                writer.writerow({k: (v if v is not None else '') for k, v in r.items()})
        sys.exit(0)
    except Exception as e:
        print('Failed writing transformed CSV:', e, file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()


