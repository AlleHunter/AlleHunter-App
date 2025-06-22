# In[ ]:


# data_processing2.py
#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import re
import numpy as np
import logging
import ast

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Normalizuje nazwy kolumn.
def _normalize_columns(df):
    df.columns = df.columns.str.strip().str.normalize('NFC')
    return df

# Ekstrahuje numer nadania.
def extract_tracking(s):
    if pd.isna(s):
        return None
    m = re.search(r"Numer nadania:\s*([A-Za-z0-9]+)", str(s))
    return m.group(1) if m else None

# Łączy DataFrame'y wysyłek.
def combine_shipments(list_of_dfs):
    if not list_of_dfs:
        logging.info("Brak list_of_dfs w combine_shipments, zwracam pusty DataFrame.")
        return pd.DataFrame()
    df = pd.concat(list_of_dfs, ignore_index=True)
    return _normalize_columns(df)

# Łączy dane wysyłek z operacjami.
def merge_shipments_and_operations(df_shipments, list_of_df_operations):
    if df_shipments.empty:
        logging.error("df_shipments jest puste w merge_shipments_and_operations.")
        return pd.DataFrame(), "Brak danych wysyłek do połączenia."
    if not list_of_df_operations:
        logging.error("list_of_df_operations jest puste w merge_shipments_and_operations.")
        return pd.DataFrame(), "Brak danych opłat do połączenia."

    df_shipments = _normalize_columns(df_shipments)
    
    df_ops_list = []
    for df_ops_part in list_of_df_operations:
        df_ops_list.append(_normalize_columns(df_ops_part))
    df_ops = pd.concat(df_ops_list, ignore_index=True)

    if df_ops.empty:
        logging.error("df_ops jest puste po połączeniu plików opłat.")
        return pd.DataFrame(), "Brak danych po połączeniu wszystkich plików opłat."

    for col in ["Data zakupu", "Data utworzenia przesyłki"]:
        if col in df_shipments.columns:
            df_shipments[col] = pd.to_datetime(df_shipments[col], errors='coerce')
    
    if "Data" in df_ops.columns:
        df_ops["Data"] = pd.to_datetime(df_ops["Data"], errors='coerce', format='%d.%m.%Y %H:%M')
    else:
        logging.error("Brak kolumny 'Data' w df_ops.")
        return pd.DataFrame(), "Brak kolumny 'Data' w plikach opłat."

    if "Szczegóły operacji" not in df_ops.columns:
        logging.error("Brak kolumny 'Szczegóły operacji' w df_ops.")
        return pd.DataFrame(), "Kolumna 'Szczegóły operacji' nie znaleziona."
    
    df_ops["Numer przesyłki"] = df_ops["Szczegóły operacji"].apply(extract_tracking)
    
    code_pickup = 'Nazwa usługi dodatkowej: Zlecenie odbioru'
    df_ops_filtered = df_ops[
        df_ops["Szczegóły operacji"].astype(str).str.contains(code_pickup, na=False)
    ].copy()

    if df_ops_filtered.empty:
        logging.warning("df_ops jest puste po filtracji opłat za podjazd. Nadal łączę dane wysyłek.")
        df_merged_temp = df_shipments.copy()
        df_merged_temp['Lista operacji'] = [[]] * len(df_merged_temp)
        return df_merged_temp, None

    fees = df_ops_filtered.groupby("Numer przesyłki")["Szczegóły operacji"].agg(list).reset_index()
    fees.columns = ["Numer przesyłki", "Lista operacji"]

    original_exists = False
    if "Lista operacji" in df_shipments.columns:
        original_exists = True
        df_shipments = df_shipments.rename(columns={"Lista operacji": "Original_Lista_Operacji"})
        logging.info("Zmieniono nazwę kolumny 'Lista operacji' w df_shipments.")

    df_merged = df_shipments.merge(fees, on="Numer przesyłki", how="left")

    def flatten(x):
        if isinstance(x, (list, tuple, set, np.ndarray, pd.Series)):
            flat=[]
            for item in x:
                if isinstance(item, (list, tuple, set, np.ndarray, pd.Series)):
                    flat.extend([str(e) for e in item if pd.notna(e)])
                else:
                    if pd.notna(item): flat.append(str(item))
            return flat
        if isinstance(x, str) and x.strip().startswith('[') and x.strip().endswith(']'):
            try:
                ev=ast.literal_eval(x)
                if isinstance(ev, (list, tuple, set)):
                    return [str(i) for i in ev if pd.notna(i)]
            except Exception:
                return []
        if pd.isna(x): return []
        return [str(x)]

    if original_exists:
        df_merged['Original_Lista_Operacji'] = df_merged['Original_Lista_Operacji'].apply(flatten)
        df_merged['Lista operacji'] = df_merged['Lista operacji'].apply(lambda x: flatten(x) if pd.notna(x) else [])
        
        df_merged['Lista operacji'] = df_merged.apply(
            lambda r: list(set(r['Original_Lista_Operacji'] + r['Lista operacji'])), axis=1)
        df_merged.drop(columns=['Original_Lista_Operacji'], inplace=True)
    else:
        df_merged['Lista operacji'] = df_merged['Lista operacji'].apply(lambda x: flatten(x) if pd.notna(x) else [])

    if df_merged.empty:
        logging.error("df_merged jest puste po łączeniu.")
        return pd.DataFrame(), "Połączenie danych nie dało wyników."

    return df_merged, None


# Analizuje opłaty DPD.
def analyze_dpd_charges(df):
    if df.empty:
        logging.error("Brak danych do analizy w analyze_dpd_charges.")
        return pd.DataFrame(), "Brak danych do analizy."

    df = _normalize_columns(df)
    
    if 'Metoda dostawy' in df.columns:
        df_dpd = df[df['Metoda dostawy'].astype(str).str.contains('DPD', na=False, case=False)].copy()
        if df_dpd.empty:
            logging.info("Brak przesyłek DPD w 'Metoda dostawy' po filtracji. Zwracam pusty DataFrame z analyze_dpd_charges.")
            return pd.DataFrame(), None 
    else:
        logging.warning("Brak kolumny 'Metoda dostawy'. Analiza będzie kontynuowana dla wszystkich przesyłek.")
        df_dpd = df.copy()

    required_columns = ['Numer przesyłki', 'Lista operacji', 'Numer zlecenia podjazdu', 'Data utworzenia przesyłki']
    if not all(col in df_dpd.columns for col in required_columns):
        missing = [col for col in required_columns if col not in df_dpd.columns]
        return pd.DataFrame(), f"Brak wymaganych kolumn dla analizy DPD: {', '.join(missing)}"

    df_dpd['Data utworzenia przesyłki'] = pd.to_datetime(df_dpd['Data utworzenia przesyłki'], errors='coerce')
    df_dpd['only_date'] = df_dpd['Data utworzenia przesyłki'].dt.normalize().dt.date
    df_dpd.dropna(subset=['only_date'], inplace=True)

    if df_dpd.empty:
        logging.info("Brak danych DPD po konwersji daty i usunięciu NaN.")
        return pd.DataFrame(), "Brak danych DPD do analizy po przetworzeniu dat."

    if 'Login' not in df_dpd.columns:
        df_dpd['Login'] = None

    results = []
    code_pickup = 'Nazwa usługi dodatkowej: Zlecenie odbioru'

    # Funkcje pomocnicze dla logiki klasyfikacji
    def check_day_pickup_types(pickup_numbers):
        pattern_onebox_allegro = re.compile(r'^\d{8}$') 
        pattern_regular_dpd = re.compile(r'^\d{14}$')

        day_has_onebox = any(pattern_onebox_allegro.fullmatch(str(x)) for x in pickup_numbers)
        day_has_regular = any(pattern_regular_dpd.fullmatch(str(x)) for x in pickup_numbers)
        day_no_explicit_pickup = not (day_has_onebox or day_has_regular)
        
        return day_has_onebox, day_has_regular, day_no_explicit_pickup

    def get_charge_comment(num_ship, day_has_onebox, day_has_regular, day_no_explicit_pickup, is_first_pickup_charge_in_day, total_pickup_charges_in_day):
        comment = ""
        is_chargeable = False

	# Klasyfikacja zgodnie ze schematem
        if num_ship <= 3:
            if day_has_onebox: 
                is_chargeable = True
                comment = "Zamówiono podjazd Allegro One Box"
                if total_pickup_charges_in_day > 1:
                    comment += ", naliczono więcej niż jedną opłatę za podjazd"
            
            elif day_has_regular: 
                if not is_first_pickup_charge_in_day:
                    is_chargeable = True
                    comment = "Naliczono więcej niż jedną opłatę za podjazd"
            
            elif day_no_explicit_pickup: 
                is_chargeable = True
                comment = "Podjazd zamówiony innym kanałem"

        elif num_ship >= 4:
            if day_has_onebox: 
                is_chargeable = True
                comment = "Kurier odebrał 4 lub więcej paczek, zamówiono podjazd Allegro One Box"
                if total_pickup_charges_in_day > 1:
                    comment += ", naliczono więcej niż jedną opłatę za podjazd"
            
            elif day_has_regular: 
                is_chargeable = True
                comment = "Kurier odebrał 4 lub więcej paczek"
                if total_pickup_charges_in_day > 1:
                    comment += ", naliczono więcej niż jedną opłatę za podjazd"
            
            elif day_no_explicit_pickup: 
                is_chargeable = True
                comment = "Podjazd zamówiony innym kanałem"
        
        return is_chargeable, comment

    for date, grp_all in df_dpd.groupby('only_date'):
        grp_unique_shipments = grp_all.drop_duplicates(subset=['Numer przesyłki']).copy()
        num_ship = len(grp_unique_shipments) 

        # Zbieramy WSZYSTKIE numery zleceń podjazdu w danej grupie dnia
        # Te numery zostaną użyte do określenia day_has_onebox, day_has_regular, day_no_explicit_pickup
        all_pickup_order_nums_in_day = grp_all['Numer zlecenia podjazdu'].astype(str).str.strip().replace(['', '-', 'nan'], np.nan).dropna().unique()
        
        day_has_onebox, day_has_regular, day_no_explicit_pickup = check_day_pickup_types(all_pickup_order_nums_in_day)

        # Filtrujemy tylko wiersze, które mają naliczoną opłatę za podjazd DPD i sortujemy po dacie utworzenia przesyłki
        pickup_rows = grp_all[grp_all['Lista operacji'].apply(
            lambda lst: any(code_pickup in str(item) for item in lst if pd.notna(item))
        )].sort_values('Data utworzenia przesyłki') 
        
        pickup_count = len(pickup_rows)

        for i, (idx, row) in enumerate(pickup_rows.iterrows()):
            current_shipment_pickup_num = str(row.get('Numer zlecenia podjazdu', '')).strip()
            
            is_chargeable, current_comment = get_charge_comment(
                num_ship, 
                day_has_onebox, 
                day_has_regular, 
                day_no_explicit_pickup, 
                (i == 0),
                pickup_count
            )
            
            if is_chargeable:
                results.append({
                    'Data Utworzenia Przesyłki': date.strftime('%Y-%m-%d'),
                    'Numer Przesyłki': row['Numer przesyłki'],
                    'Login': row.get('Login', None),
                    'Komentarz': current_comment.strip(),
                    'Liczba Przesyłek Danego Dnia': num_ship
                })
    
    # Konwersja listy słowników na DataFrame            
    final_df = pd.DataFrame(results)

    # Dodanie kolumny z indeksem od 1
    if not final_df.empty:
        final_df.insert(0, 'Lp.', range(1, 1 + len(final_df)))
        
    return final_df, None