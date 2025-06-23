#!/usr/bin/env python
# coding: utf-8

#app2.py
import streamlit as st
import pandas as pd
import io
import datetime
import pymysql
from data_processing2 import combine_shipments, merge_shipments_and_operations, analyze_dpd_charges

# Konfiguracja strony
st.set_page_config(
    page_title="AlleHunter",
    page_icon="assets/my_favicon_256px.png"
)


# Niestandardowe style CSS
st.markdown(
    """
    <style>
    .st-emotion-cache-nahz7x {
        display: flex;
        align-items: center;
    }

    .stDownloadButton > button {
        background-color: #4CAF50;
        color: white;
        padding: 10px 20px;
        border: none;
        border-radius: 5px;
        cursor: pointer;
        font-size: 16px;
        transition: background-color 0.3s ease;
    }

    .stDownloadButton > button:hover {
        background-color: #45a049;
    }

    .stDownloadButton > button:active {
        background-color: #3e8e41;
    }

    div.stButton button[data-testid*="stButton-primary"] {
        background-color: #dc3545;
        color: white;
        padding: 10px 20px;
        border: none;
        border-radius: 5px;
        cursor: pointer;
        font-size: 16px;
        transition: background-color 0.3s ease;
    }

    div.stButton button[data-testid*="stButton-primary"]:hover {
        background-color: #c82333;
    }

    div.stButton button[data-testid*="stButton-primary"]:active {
        background-color: #bd2130;
    }

    .download-button-container {
        margin-top: 20px;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# Funkcja do nawiązywania połączenia z MySQL
def get_mysql_connection():
    cfg = st.secrets["connections"]["mysql"]
    try:
        conn = pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["username"],
            password=cfg["password"],
            database=cfg["database"],
            connect_timeout=10,
            read_timeout=10, # Dodanie read_timeout
            write_timeout=10 # Dodanie write_timeout
        )
        return conn
    except pymysql.MySQLError as err:
        st.error(f"Błąd MySQL: {err}. Spróbuj odświeżyć stronę lub skontaktuj się z administratorem.")
        return None
    except Exception as e:
        st.error(f"Nieoczekiwany błąd połączenia: {e}. Spróbuj odświeżyć stronę lub skontaktuj się z administratorem.")
        return None

# Funkcja testująca połączenie i próbująca je odnowić
def get_active_db_connection():
    # Sprawdzamy, czy połączenie już istnieje w sesji i jest aktywne
    if 'db_connection' not in st.session_state or st.session_state.db_connection is None:
        st.session_state.db_connection = get_mysql_connection()
    else:
        try:
            # Ping, aby sprawdzić, czy połączenie jest nadal aktywne
            st.session_state.db_connection.ping(reconnect=True)
        except pymysql.Error:
            # Jeśli ping się nie powiedzie, próbujemy nawiązać nowe połączenie
            st.session_state.db_connection = get_mysql_connection()
    return st.session_state.db_connection


# Funkcje globalnego licznika
@st.cache_data(ttl=600) # Cache'ujemy dane na 10 minut (600 sekund)
def get_global_dpd_errors_count_cached():
    conn = get_active_db_connection()
    if conn is None:
        return 0
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT count FROM global_counter WHERE counter_name = 'dpd_errors_total'"
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                # Inicjalizujemy licznik, jeśli nie istnieje
                cursor.execute(
                    "INSERT IGNORE INTO global_counter (counter_name, count) VALUES ('dpd_errors_total', 0)"
                )
                conn.commit()
                return 0
    except pymysql.MySQLError as err:
        st.warning(f"Błąd podczas pobierania globalnego licznika: {err}")
        return 0
    finally:
        pass


def update_global_dpd_errors_count(increment_by):
    conn = get_active_db_connection() # Używamy funkcji testującej połączenie
    if conn is None:
        return
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE global_counter SET count = count + %s WHERE counter_name = 'dpd_errors_total'",
                (increment_by,)
            )
            conn.commit()
        # Ważne: Po aktualizacji danych w bazie, czyścimy cache, aby wymusić odczyt nowych danych
        get_global_dpd_errors_count_cached.clear()
    except pymysql.MySQLError as err:
        st.error(f"Błąd podczas aktualizacji globalnego licznika: {err}")

# Kolumny
col_logo, col_counter = st.columns([3, 2])

with col_logo:
    st.image("assets/my_logo_500px.png", width=300)

with col_counter:
    st.markdown("<div style='margin-top: 90px;'>", unsafe_allow_html=True)
    
    # Pobieramy licznik z funkcji cachującej
    global_count = get_global_dpd_errors_count_cached() 
    st.markdown(f"<p style='text-align: center; font-size: 1.1em;'>TYLE niesłusznych opłat Allegro DPD wykrył dotąd AlleHunter</p>", unsafe_allow_html=True)
    st.markdown(f"<h2 style='text-align: center; margin-top: -15px;'>{global_count}</h2>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if st.button("Odśwież licznik", key="refresh_counter_button", help="Kliknij, aby odświeżyć globalny licznik wykrytych opłat"):
        get_global_dpd_errors_count_cached.clear()

# Sekcja: Opis / Instrukcja
with st.expander("Jak przygotować dane i używać AlleHunter?"):
    st.markdown(
        """
        ### Witaj w Panelu Aplikacji **AlleHunter**
        
        Aplikacja jest **ZUPEŁNIE DARMOWYM** narzędziem analitycznym, powstałym, aby umożliwić Sprzedającym na portalu Allegro szybkie sprawdzenie poprawności naliczonych przez serwis opłat z tytułu podjazdów kurierskich DPD. Stanowi odpowiedź na przeszkody stawiane przed Sprzedającymi w zakresie sprawdzania udostępnianych im zestawień i raportów, wyrównuje szanse w zakresie analizy danych i eliminuje konieczność wielotygodniowego, ręcznego rozpisywania i zestawiania tysięcy rekordów.
        
        Działanie aplikacji opiera się na grupowaniu wszystkich przesyłek DPD, niezależnie od rodzaju, z podziałem na dni i sprawdzeniu, czy w danym dniu naliczono opłatę za pojazd kuriera, oraz czy jest to uzasadnione. Działa zarówno w przypadku, gdy sprzedajesz na jednym koncie, ale również w odniesniu do wielu kont sprzedażowych, łącząc dane z dowolnej ich liczby, co jest absolutnie fundamentalne, gdy chcemy uzyskać poprawne dane wynikowe.
        
        Allegro udostępnia do pobrania zestawienia, z których aplikacja wyodrębni te przesyłki, które potencjalnie zostały błędnie obciążone opłatą i przedstawi wynik w formie pliku CSV do pobrania, który można bezpośrednio przesłać do działu reklamacji Allegro, bez konieczności składania reklamacji dla każdej przesyłki osobno. Wykryte opłaty są komentowane, by wskazać na jakiej podstawie zostały wytypowane do zgłoszenia. Klucz klasyfikacji, według którego działa aplikacja, znajduje się w osobnej zakładce.
        
        
        ### **Przygotowanie plików z Allegro**
        
        Aby **AlleHunter** mógł poprawnie przeanalizować Twoje dane i wskazać potencjalnie nieprawidłowe opłaty za podjazdy DPD,
        przygotuj i załaduj dwa rodzaje plików CSV:

        **1. Pliki z wysyłkami (Zestawienie dostaw) - Jeśli masz więcej niż jedno konto Allegro, *KONIECZNIE* ze wszystkich swoich kont sprzedażowych Allegro:**
        
        * Pobierz 'Zestawienie dostaw' z Panelu Allegro (SalesCenter > Zamówienia i zwroty > Zamówione przesyłki).
        
        * Allegro pozwala na pobranie naraz 100 pozycji, dlatego ustaw liczbę wyświetlanych operacji na 100 (kolejne pliki pobierzesz na kolejnych stronach)".
        
        * Zaznacz wszystkie pozycje na stronie i wybierz "POBIERZ PLIK Z DANYMI"
        
        * Wybierz filtry: Data zakupu, Data utworzenia przesyłki, Metoda dostawy, Przewoźnik, Numer przesyłki, Numer zamówienia, Login, Rodzaj przesyłki, Wymiary, Waga, Kwota pobrania, Usługi dodatkowe, Numer zlecenia podjazdu, USTAW FORMAT PLIKU NA CSV!.
        
        * Kliknij "POBIERZ PLIK". Aby pobrać kolejne fragmety zestawienia, musisz przejść na kolejną stronę i powtórzyć ostatnie kroki (Na momet pisania aplikacji Allegro nie umożliwa pobrania więcej niż 100 rekordów na raz)
        
        * Dla wygody wszystkie pliki z zestawieniami dostaw zapisz w jednym folderze (ułatwi to ich masowe ładowanie do aplikacji)
        
        * W Panelu możesz pobrać zestawienie z ostatniego roku i taki zakres polecamy dla efektywnej analizy.
        
        * Załaduj pliki - Możesz załadować dowolną liczbę plików, z dowolnej liczby kont sprzedażowych. **PAMIĘTAJ!** - jeśli prowdzisz sprzedaż na wielu kontach Allegro, to MUSISZ załączyć WSZYSTKIE pliki ze WSZYSTKICH kont - **Jest to absolutnie konieczne dla poprawności wyników raportu!** **2. Pliki z opłatami (Zestawienie opłat) - Jeśli masz więcej niż jedno, **KONIECZNIE** ze wszystkich swoich kont sprzedażowych Allegro:**


        **2. Pliki z opłatami (Zestawienie opłat) - Jeśli masz więcej niż jedno konto Allegro, *KONIECZNIE* ze wszystkich swoich kont sprzedażowych Allegro:**
        * Pobierz 'Zestawienie opłat' z Panelu Allegro (SalesCenter > Finanse > Rozliczenia z Allegro).
        
        * W sekcji "Za okres" ustaw "wybierz". Następnie ustaw datę w zakresie, która Cię interesuje (polecamy ostatni rok)
        
        * Kliknij "WYGENERUJ ZESTAWIENIE" i poczekaj aż Allegro prześlesze je do Ciebie na e-mail.
        
        * Dla wygody wszystkie pliki z zestawieniami opłat zapisz w jednym folderze (ułatwi to ich masowe ładowanie do aplikacji)
        
        * Załaduj pliki - Możesz załadować dowolną liczbę plików, z dowolnej liczby kont sprzedażowych. **PAMIĘTAJ!** - jeśli prowdzisz sprzedaż na wielu kontach Allegro, to MUSISZ załączyć WSZYSTKIE pliki ze WSZYSTKICH kont - **Jest to absolutnie konieczne dla poprawności wyników raportu!** 
        
        ### **Instrukcja użytkowania:**
        1.  Załaduj swoje pliki z wysyłkami w sekcji "Tutaj załaduj wszystkie swoje pliki z wysyłkami". **Pamiętaj** aby wgrać każdy plik tylko raz.
        2.  Załaduj swoje pliki z opłatami w sekcji "Tutaj załaduj wszystkie swoje pliki z opłatami". **Pamiętaj** aby wgrać każdy plik tylko raz.
        3.  Kliknij przycisk "Analizuj opłaty DPD".
        4.  Poczekaj na przetworzenie danych. Wyniki zostaną wyświetlone poniżej. Aplikacja ma wbudowaną przeglądarkę plików csv, dzięki której możesz przeszukiwać plik wynikowy, a także przeglądać go natychmiast po wygenerowaniu.
        5.  Na koniec możesz pobrać wyniki w formacie CSV.
        """,
        unsafe_allow_html=True
    )

# Sekcja: Schemat klasyfikowania opłat
with st.expander("Schemat klasyfikowania opłat"):
    st.markdown(
        """
        ### Schemat klasyfikowania opłat

        Poniżej znajduje się schemat, według którego AlleHunter typuje niesłusznie naliczone opłaty. **Klucz klasyfikacji** pozwala na sprawdzenie wyników działania aplikacji i upewnienie się przed złożeniem reklamacji, że przedstawione wyniki są poprawne.

        **WYJAŚNIENIE POJĘĆ:**
        - **Podjazd DPD (normalny)** - Podjazd DPD powiązany z dowolną usługą DPD, poza Allegro One Box, rozliczaną w ramach Allegro - Dla 3 lub mniej przesyłek **PŁATNY** - Dla 4 i więcej przesyłek **BEZPŁATNY**
        - **Podjazd Allegro One Box** - Podjazd DPD zamówiony w ramach Allegro One Box - **BEZPŁATNY** niezależnie od ilości paczek
        - **Podjazd zamówiony innym kanałem** - Podjazd rozliczany poza Allegro - Zlecenie stałe w ramach własnej umowy, Podjazd zamówiony w zewnętrznym serwisie - np Apaczka, Epaka, Sendit.  

        **Klucz klasyfikacji:**

        1. 3 lub mniej przesyłek - zamówiono podjazd Allegro One Box - Jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Zamówiono podjazd Allegro One Box

        2. 3 lub mniej przesyłek - zamówiono podjazd Allegro One Box - Więcej niż jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Każda przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Zamówiono podjazd Allegro One Box, naliczono więcej niż jedną opłatę za podjazd

        3. 3 lub mniej przesyłek - zamówiono podjazd DPD (normalny)- Jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Przesyłka z naliczoną opłatą nie trafi do zestawienia jako błędnie obciążona - Komentarz: -

        4. 3 lub mniej przesyłek - zamówiono podjazd DPD (normalny)- Więcej niż jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Pierwsza przesyłka z naliczoną opłatą nie trafi do zestawienia jako błędnie obciążona, każda kolejna przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Naliczono więcej niż jedną opłatę za podjazd

        5. 3 lub mniej przesyłek - zamówiono więcej niż jeden podjazd w grupie ale wśród nich jest numer podjazdu Allegro One Box - Jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Zamówiono podjazd Allegro One Box

        6. 3 lub mniej przesyłek - zamówiono więcej niż jeden podjazd w grupie ale wśród nich jest numer podjazdu Allegro One Box - Więcej niż jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Każda przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Zamówiono podjazd Allegro One Box, naliczono więcej niż jedną opłatę za podjazd

        7. 3 lub mniej przesyłek - zamówiono więcej niż jeden podjazd w grupie ale wśród nich są tylko numery podjazdu DPD (normalne) - Jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Przesyłka z naliczoną opłatą nie trafi do zestawienia jako błędnie obciążona - Komentarz: -

        8. 3 lub mniej przesyłek - zamówiono więcej niż jeden podjazd w grupie ale wśród nich są tylko numery podjazdu DPD (normalne) - Więcej niż jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Pierwsza przesyłka z naliczoną opłatą nie trafi do zestawienia jako błędnie obciążona, każda kolejna przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Naliczono więcej niż jedną opłatę za podjazd

        9. 4 lub więcej przesyłek - zamówiono podjazd Allegro One Box - Jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Zamówiono podjazd Allegro One Box

        10. 4 lub więcej przesyłek - zamówiono podjazd Allegro One Box - Więcej niż jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Każda przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Zamówiono podjazd Allegro One Box, naliczono więcej niż jedną opłatę za podjazd

        11. 4 lub więcej przesyłek - zamówiono podjazd DPD (normalny)- Jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Kurier odebrał 4 lub więcej paczek

        12. 4 lub więcej przesyłek - zamówiono podjazd DPD (normalny)- Więcej niż jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Każda przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Kurier odebrał 4 lub więcej paczek, naliczono więcej niż jedną opłatę za podjazd

        13. 4 lub więcej przesyłek - zamówiono więcej niż jeden podjazd w grupie ale wśród nich jest numer podjazdu Allegro One Box - Jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Kurier odebrał 4 lub więcej paczek, zamówiono podjazd Allegro One Box

        14. 4 lub więcej przesyłek - zamówiono więcej niż jeden podjazd w grupie ale wśród nich jest numer podjazdu Allegro One Box - Więcej niż jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Każda przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Kurier odebrał 4 lub więcej paczek, zamówiono podjazd Allegro One Box, naliczono więcej niż jedną opłatę za podjazd

        15. 4 lub więcej przesyłek - zamówiono więcej niż jeden podjazd w grupie ale wśród nich są tylko numery podjazdu DPD (normalne) - Jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Kurier odebrał 4 lub więcej przesyłek

        16. 4 lub więcej przesyłek - zamówiono więcej niż jeden podjazd w grupie ale wśród nich są tylko numery podjazdu DPD (normalne) - Więcej niż jedna przesyłka w grupie ma naliczoną opłatę - Klasyfikacja: Każda przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Kurier odebrał 4 lub więcej paczek, naliczono więcej niż jedną opłatę za podjazd

        17. 3 lub mniej przesyłek - Nie zamówiono żadnego podjazdu - Jedna lub więcej przesyłek w grupie ma naliczoną opłatę - Klasyfikacja: Każda przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Zamówiono podjazd innym kanałem

        18. 4 lub więcej przesyłek - Nie zamówiono żadnego podjazdu - Jedna lub więcej przesyłek w grupie ma naliczoną opłatę - Klasyfikacja: Każda przesyłka z naliczoną opłatą trafi do zestawienia jako błędnie obciążona - Komentarz: Zamówiono podjazd innym kanałem
        """,
        unsafe_allow_html=True
    )

# Sekcja "Bezpieczeństwo Twoich danych"
with st.expander("Bezpieczeństwo Twoich danych"):
    st.markdown(
        """
        ### Bezpieczeństwo Twoich danych  
        
        Wszystkie przesyłane przez Ciebie pliki są przetwarzane przez AlleHunter **wyłącznie w Twojej przeglądarce** i **nie są nigdzie zapisywane ani przesyłane na żadne serwery zewnętrzne**.
        Aplikacja działa lokalnie, a po zamknięciu strony wszystkie dane, które zostały załadowane, zostają usunięte z pamięci.
        Jedyną informacją przechowywaną w zewnętrznej bazie danych jest **globalna, anonimowa liczba wykrytych błędnych opłat naliczonych przez Allegro**, która służy jedynie do celów statystycznych i nie jest w żaden sposób powiązana z Twoimi danymi.  

        **Jeśli masz jakieś pytania, znalazłeś błąd lub masz sugestię dotyczącą aplikacji --> allehunter@tutamail.com**
        """,
        unsafe_allow_html=True
    )


# Sekcja ładowania plików
st.header("Załaduj pliki")

uploaded_shipments_files = st.file_uploader(
    "Tutaj załaduj wszystkie swoje pliki z wysyłkami - KONIECZNIE ZE WSZYSTKICH SWOICH KONT SPRZEDAŻOWYCH - format CSV",
    type="csv",
    accept_multiple_files=True
)

uploaded_operations_files = st.file_uploader(
    "Tutaj załaduj wszystkie swoje pliki z opłatami- KONIECZNIE ZE WSZYSTKICH SWOICH KONT SPRZEDAŻOWYCH - format CSV",
    type="csv",
    accept_multiple_files=True
)

df_results = pd.DataFrame()

if st.button("Analizuj opłaty DPD"):
    if not uploaded_shipments_files:
        st.error("Proszę załadować pliki z wysyłkami.")
    elif not uploaded_operations_files:
        st.error("Proszę załadować pliki z opłatami.")
    else:
        with st.spinner("Przetwarzam dane... Może to chwilę potrwać..."):
            processed_shipment_file_names = set()
            processed_operation_file_names = set()
            
            shipments_dfs = []
            operations_dfs = []
            
            # Flaga do wykrycia duplikatów lub błędów ładowania
            load_aborted = False 

            # Wczytywanie plików wysyłek z kontrolą duplikatów
            for file in uploaded_shipments_files:
                if file.name in processed_shipment_file_names:
                    st.warning(f"Plik wysyłki '{file.name}' został już załadowany.")
                    load_aborted = True
                    continue
                processed_shipment_file_names.add(file.name)
                try:
                    df = pd.read_csv(file, encoding='utf-8')
                    shipments_dfs.append(df)
                except UnicodeDecodeError:
                    df = pd.read_csv(file, encoding='latin1')
                    shipments_dfs.append(df)
                except Exception as e:
                    if "Error tokenizing data" in str(e) or ("Expected" in str(e) and "fields" in str(e) and "saw" in str(e)):
                        st.error(f"Prawdopodobny błąd w pliku wysyłki '{file.name}'. Upewnij się, że załadowano pliki do odpowiednich sekcji (pliki wysyłek tutaj, pliki opłat w drugiej sekcji).")
                    else:
                        st.error(f"Wystąpił błąd podczas wczytywania pliku wysyłki '{file.name}': {e}")
                    load_aborted = True
                    continue

            # Wczytywanie plików opłat z kontrolą duplikatów
            for file in uploaded_operations_files:
                if file.name in processed_operation_file_names:
                    st.warning(f"Plik opłat '{file.name}' został już załadowany.")
                    load_aborted = True
                    continue
                processed_operation_file_names.add(file.name)
                try:
                    df = pd.read_csv(file, encoding='utf-8', sep=';', decimal=',')
                    operations_dfs.append(df)
                except UnicodeDecodeError:
                    df = pd.read_csv(file, encoding='latin1', sep=';', decimal=',')
                    operations_dfs.append(df)
                except Exception as e:
                    # Sprawdzamy, czy błąd zawiera frazę wskazującą na problem z formatem CSV
                    if "Error tokenizing data" in str(e) or ("Expected" in str(e) and "fields" in str(e) and "saw" in str(e)):
                        st.error(f"Prawdopodobny błąd w pliku opłat '{file.name}'. Upewnij się, że załadowano pliki do odpowiednich sekcji (pliki opłat tutaj, pliki wysyłek w pierwszej sekcji).")
                    else:
                        st.error(f"Wystąpił błąd podczas wczytywania pliku opłat '{file.name}': {e}")
                    load_aborted = True
                    continue

            # Kontynuujemy przetwarzanie tylko jeśli nie było żadnych problemów z ładowaniem (duplikaty, błędy)
            if load_aborted:
                st.error("Wykryto problemy z plikami. Proszę przeładować aplikację i wgrać ponownie tylko poprawne pliki do odpowiednich sekcji.")
            elif not shipments_dfs:
                st.error("Brak poprawnych plików wysyłek do przetworzenia.")
            elif not operations_dfs:
                st.error("Brak poprawnych plików opłat do przetworzenia.")
            else:
                df_shipments_combined = combine_shipments(shipments_dfs)
                df_merged, merge_error = merge_shipments_and_operations(
                    df_shipments_combined, operations_dfs
                )

                if merge_error:
                    st.error(f"Błąd podczas łączenia danych: {merge_error}")
                elif df_merged.empty:
                    st.warning("Brak danych po połączeniu. Sprawdź numery przesyłek i pliki.")
                else:
                    df_results, analyze_error = analyze_dpd_charges(df_merged)
                    
                    if analyze_error:
                        st.error(f"Błąd podczas analizy opłat DPD: {analyze_error}")
                    elif df_results.empty:
                        st.info("Nie znaleziono żadnych podejrzanych opłat DPD.")
                    else:
                        st.success(f"Znaleziono {len(df_results)} podejrzanych opłat DPD!")
                        # Aktualizujemy globalny licznik
                        update_global_dpd_errors_count(len(df_results)) 

# Sekcja wyświetlania wyników i pobierania pliku
if not df_results.empty:
    st.warning(
        "**Uwaga:** Wyniki należy zweryfikować we własnym zakresie przed złożeniem reklamacji."
    )

    st.dataframe(df_results)

    csv_buffer = io.StringIO()
    df_results.to_csv(
        csv_buffer,
        index=False,
        encoding='utf-8-sig'
    )
    
    # Otaczamy przycisk w kontener z niestandardową klasą CSS
    st.markdown('<div class="download-button-container">', unsafe_allow_html=True)
    st.download_button(
        label="Pobierz wyniki jako CSV",
        data=csv_buffer.getvalue(),
        file_name=(
            f"Zestawienie_Blednie_Naliczonych_Oplat_DPD_"
            f"{datetime.date.today():%Y-%m-%d}.csv"
        ),
        mime="text/csv",
        key="download_csv_button" 
    )
    st.markdown('</div>', unsafe_allow_html=True)
