import os
import re
import sys
from datetime import datetime

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Add parent directory to sys.path to ensure imports work correctly
parent_dir = os.path.dirname(os.path.abspath(__file__))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Import models with a try-except to handle both direct execution and module import
try:
    # Try direct import first (for Celery worker)
    from www.models import (
        Darowizny,
        Dziedziczenia,
        Egzekucje,
        Hipoteki,
        Informacje,
        Spadki,
        Wlasciciele,
    )
except ImportError:
    # If that fails, try to import from the correct path based on file location
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.exists(os.path.join(current_dir, "www", "models.py")):
        sys.path.append(current_dir)
        from www.models import (
            Darowizny,
            Dziedziczenia,
            Egzekucje,
            Hipoteki,
            Informacje,
            Spadki,
            Wlasciciele,
        )
    else:
        sys.exit(1)

load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Create the tables if they don't exist
Base.metadata.create_all(engine)


def parse_directory(subdirectory_path, **kwargs):
    # Initialize session
    print(f"[Parser] START: Parsing directory {subdirectory_path}")
    print(f"[Parser] DB URL: {DATABASE_URL}")
    Session = sessionmaker(bind=engine)
    session = Session()

    # Use the exact same path logic as in your original script
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    MAIN_DIR = os.path.join(CURRENT_DIR, "ekw")

    def format_kwota(kwota):
        buffer = kwota.split("(")
        return buffer[0] + str(buffer[1]).split(")")[1].strip()

    # Function to calculate age based on PESEL
    def calculate_age(pesel):
        year = int(pesel[0:2])
        month = int(pesel[2:4])
        day = int(pesel[4:6])

        # Adjust year and month based on PESEL
        if month > 20:
            year += 2000
            month -= 20
        else:
            year += 1900

        try:
            birth_date = datetime(year, month, day)
        except ValueError:
            return "Brak"

        today = datetime.today()
        age = (
            today.year
            - birth_date.year
            - ((today.month, today.day) < (birth_date.month, birth_date.day))
        )

        return age

    def process_kw(file_path, data, wlasciciele):
        kw = ""
        egzekucje = []  # Add list to store execution data
        hipoteki = []  # Add list to store mortgage data
        spadki = []  # Add list to store inheritance data
        dziedziczenia = []  # Add list to store inheritance by act data
        darowizny = []  # Add list to store gift contract data
        # Start parsing the HTML file
        with open(file_path, "r", encoding="utf-8") as file:
            html_content = file.read()

        # Clean up <script> tags
        soup = BeautifulSoup(html_content, "html.parser")
        for script in soup.find_all("script"):
            script.decompose()

        # Update CSS style paths
        for link in soup.find_all("link"):
            if "style.css" in link.get("href"):
                link["href"] = "../../style.css"

        # Remove the table with id="nawigacja"
        try:
            soup.find("table", id="nawigacja").decompose()
        except AttributeError:
            pass

        # Remove all <form> tags
        for form in soup.find_all("form"):
            form.decompose()

        try:
            kw = soup.find("b").get_text().replace("/", "-")
        except AttributeError:
            print(f"[Parser] ERROR: Could not find KW number in {file_path}")
            # print first 500 chars to debug
            print(f"[Parser] HTML Content Preview: {html_content[:500]}...")
            return None

        try:
            typ = soup.find("h3").get_text()
            data["Typ"] = typ
        except AttributeError:
            return None

        umowa_darowizny = 0
        nabycie_spadku = 0
        dziedziczenie = 0
        wszczecie_egzekucji = 0

        if "DIII.html" in file_path:
            div_content = soup.find("div", id="contentDzialu")
            if div_content:
                # Dictionary to store execution basis numbers and their details
                execution_entries = {}

                # First pass: collect all basis numbers and mark execution entries
                for table in div_content.find_all("table", class_="tbOdpis"):
                    current_nr_podstawy = None

                    for row in table.find_all("tr"):
                        cells = row.find_all("td")

                        # Look for "Nr podstawy wpisu" header and find basis numbers in subsequent rows
                        for i, cell in enumerate(cells):
                            if "Nr podstawy wpisu" in cell.get_text():
                                # Look for basis numbers in the same row or following rows
                                # Check remaining cells in current row
                                for j in range(i + 1, len(cells)):
                                    cell_text = cells[j].get_text().strip()
                                    if cell_text.isdigit():
                                        current_nr_podstawy = cell_text
                                        execution_entries[current_nr_podstawy] = {
                                            "details": None
                                        }
                                        break

                        # Check if current row has a basis number in a cell with specific positioning
                        for cell in cells:
                            cell_text = cell.get_text().strip()
                            # Look for standalone numbers that could be basis numbers
                            if cell_text.isdigit() and len(cell_text) <= 3:
                                # Check if this cell might be a basis number by looking at row structure
                                if cell.get("rowspan") or "csDane" in cell.get(
                                    "class", []
                                ):
                                    current_nr_podstawy = cell_text
                                    if current_nr_podstawy not in execution_entries:
                                        execution_entries[current_nr_podstawy] = {
                                            "details": None
                                        }

                        # Look for execution text and associate with current basis number
                        if (
                            "WSZCZĘCIU EGZEKUCJI" in row.get_text()
                            or "WSZCZĘTA EGZEKUCJA" in row.get_text()
                        ):
                            wszczecie_egzekucji = 1
                            data["Wszczęcie egzekucji"] = 1

                            # Mark the current basis number as execution-related
                            if (
                                current_nr_podstawy
                                and current_nr_podstawy in execution_entries
                            ):
                                execution_entries[current_nr_podstawy][
                                    "is_execution"
                                ] = True

                # Second pass: find document details for execution entries
                for table in div_content.find_all("table", class_="tbOdpis"):
                    for row in table.find_all("tr"):
                        cells = row.find_all("td")

                        # Look for cells that contain basis numbers
                        for i, cell in enumerate(cells):
                            cell_text = cell.get_text().strip()

                            # Check if this cell contains a basis number that's marked for execution
                            if cell_text in execution_entries and execution_entries[
                                cell_text
                            ].get("is_execution"):
                                # Find the document details - usually in the next cell or next row
                                details_cell = None

                                # Try next cell in same row
                                if i + 1 < len(cells):
                                    details_cell = cells[i + 1]

                                # If not found, try looking in next row
                                if (
                                    not details_cell
                                    or not details_cell.get_text().strip()
                                ):
                                    next_row = row.find_next_sibling("tr")
                                    if next_row:
                                        next_cells = next_row.find_all("td")
                                        if next_cells:
                                            details_cell = (
                                                next_cells[-1]
                                                if len(next_cells) > 1
                                                else next_cells[0]
                                            )

                                if details_cell:
                                    details_text = details_cell.get_text()

                                    # Extract date using regex (format: YYYY-MM-DD)
                                    date_match = re.search(
                                        r"(\d{4}-\d{2}-\d{2})", details_text
                                    )
                                    if date_match:
                                        date_str = date_match.group(1)

                                        egzekucja_entry = {
                                            "kw": kw,
                                            "data_wszczecia": date_str,
                                        }
                                        egzekucje.append(egzekucja_entry)

        # Retrieve owner information
        if "DII.html" in file_path:
            div_content = soup.find("div", id="contentDzialu")
            if div_content:
                # Dictionary to store inheritance basis numbers and their details
                inheritance_entries = {}

                # First pass: collect all basis numbers and identify inheritance-related entries
                for table in div_content.find_all("table", class_="tbOdpis"):
                    current_nr_podstawy = None

                    for row in table.find_all("tr"):
                        cells = row.find_all("td")

                        # Look for "Nr podstawy wpisu" header and find basis numbers in subsequent rows
                        for i, cell in enumerate(cells):
                            if "Nr podstawy wpisu" in cell.get_text():
                                # Look for basis numbers in the same row or following rows
                                for j in range(i + 1, len(cells)):
                                    cell_text = cells[j].get_text().strip()
                                    if cell_text.isdigit():
                                        current_nr_podstawy = cell_text
                                        inheritance_entries[current_nr_podstawy] = {
                                            "details": None
                                        }
                                        break

                        # Check if current row has a basis number in a cell with specific positioning
                        for cell in cells:
                            cell_text = cell.get_text().strip()
                            # Look for standalone numbers that could be basis numbers
                            if cell_text.isdigit() and len(cell_text) <= 3:
                                # Check if this cell might be a basis number by looking at row structure
                                if cell.get("rowspan") or "csDane" in cell.get(
                                    "class", []
                                ):
                                    current_nr_podstawy = cell_text
                                    if current_nr_podstawy not in inheritance_entries:
                                        inheritance_entries[current_nr_podstawy] = {
                                            "details": None
                                        }

                        # Look for inheritance-related text and associate with current basis number
                        row_text = row.get_text()
                        if any(
                            phrase in row_text
                            for phrase in [
                                "POSTANOWIENIE O STWIERDZENIU NABYCIA SPADKU",
                                "POSTANOWIENIE O DZIALE SPADKU",
                                "ZMIENIAJĄCE POSTANOWIENIE O STWIERDZENIU NABYCIA SPADKU",
                            ]
                        ):
                            nabycie_spadku = 1
                            data["Nabycie spadku"] = 1

                            # Mark the current basis number as inheritance-related
                            if (
                                current_nr_podstawy
                                and current_nr_podstawy in inheritance_entries
                            ):
                                inheritance_entries[current_nr_podstawy][
                                    "is_inheritance"
                                ] = True

                        if "UMOWA DAROWIZNY" in row_text:
                            umowa_darowizny = 1
                            data["Umowa darowizny"] = 1

                            # Mark the current basis number as gift contract-related
                            if (
                                current_nr_podstawy
                                and current_nr_podstawy in inheritance_entries
                            ):
                                inheritance_entries[current_nr_podstawy][
                                    "is_darowizna"
                                ] = True

                        if "AKT POŚWIADCZENIA DZIEDZICZENIA" in row_text:
                            dziedziczenie = 1
                            data["Dziedziczenie"] = 1

                            # Mark the current basis number as inheritance by act-related
                            if (
                                current_nr_podstawy
                                and current_nr_podstawy in inheritance_entries
                            ):
                                inheritance_entries[current_nr_podstawy][
                                    "is_dziedziczenie"
                                ] = True

                # Second pass: find document details for inheritance entries
                for table in div_content.find_all("table", class_="tbOdpis"):
                    for row in table.find_all("tr"):
                        cells = row.find_all("td")

                        # Look for cells that contain basis numbers
                        for i, cell in enumerate(cells):
                            cell_text = cell.get_text().strip()

                            # Check if this cell contains a basis number that's marked for inheritance
                            if cell_text in inheritance_entries and inheritance_entries[
                                cell_text
                            ].get("is_inheritance"):
                                # Find the document details - usually in the next cell or next row
                                details_cell = None

                                # Try next cell in same row
                                if i + 1 < len(cells):
                                    details_cell = cells[i + 1]

                                # If not found, try looking in next row
                                if (
                                    not details_cell
                                    or not details_cell.get_text().strip()
                                ):
                                    next_row = row.find_next_sibling("tr")
                                    if next_row:
                                        next_cells = next_row.find_all("td")
                                        if next_cells:
                                            details_cell = (
                                                next_cells[-1]
                                                if len(next_cells) > 1
                                                else next_cells[0]
                                            )

                                if details_cell:
                                    details_text = details_cell.get_text()

                                    # Extract date using regex (format: YYYY-MM-DD)
                                    date_match = re.search(
                                        r"(\d{4}-\d{2}-\d{2})", details_text
                                    )
                                    if date_match:
                                        date_str = date_match.group(1)

                                        spadek_entry = {
                                            "kw": kw,
                                            "data_orzeczenia": date_str,
                                        }
                                        spadki.append(spadek_entry)

                            # Check if this cell contains a basis number that's marked for gift contract
                            if cell_text in inheritance_entries and inheritance_entries[
                                cell_text
                            ].get("is_darowizna"):
                                # Find the document details - usually in the next cell or next row
                                details_cell = None

                                # Try next cell in same row
                                if i + 1 < len(cells):
                                    details_cell = cells[i + 1]

                                # If not found, try looking in next row
                                if (
                                    not details_cell
                                    or not details_cell.get_text().strip()
                                ):
                                    next_row = row.find_next_sibling("tr")
                                    if next_row:
                                        next_cells = next_row.find_all("td")
                                        if next_cells:
                                            details_cell = (
                                                next_cells[-1]
                                                if len(next_cells) > 1
                                                else next_cells[0]
                                            )

                                if details_cell:
                                    details_text = details_cell.get_text()

                                    # Extract date using regex (format: YYYY-MM-DD)
                                    date_match = re.search(
                                        r"(\d{4}-\d{2}-\d{2})", details_text
                                    )
                                    if date_match:
                                        date_str = date_match.group(1)

                                        darowizna_entry = {
                                            "kw": kw,
                                            "data_umowy": date_str,
                                        }
                                        darowizny.append(darowizna_entry)

                            # Check if this cell contains a basis number that's marked for inheritance by act
                            if cell_text in inheritance_entries and inheritance_entries[
                                cell_text
                            ].get("is_dziedziczenie"):
                                # Find the document details - usually in the next cell or next row
                                details_cell = None

                                # Try next cell in same row
                                if i + 1 < len(cells):
                                    details_cell = cells[i + 1]

                                # If not found, try looking in next row
                                if (
                                    not details_cell
                                    or not details_cell.get_text().strip()
                                ):
                                    next_row = row.find_next_sibling("tr")
                                    if next_row:
                                        next_cells = next_row.find_all("td")
                                        if next_cells:
                                            details_cell = (
                                                next_cells[-1]
                                                if len(next_cells) > 1
                                                else next_cells[0]
                                            )

                                if details_cell:
                                    details_text = details_cell.get_text()

                                    # Extract date using regex (format: YYYY-MM-DD)
                                    date_match = re.search(
                                        r"(\d{4}-\d{2}-\d{2})", details_text
                                    )
                                    if date_match:
                                        date_str = date_match.group(1)

                                        dziedziczenie_entry = {
                                            "kw": kw,
                                            "data_orzeczenia": date_str,
                                        }
                                        dziedziczenia.append(dziedziczenie_entry)

                for row in div_content.find_all("tr"):
                    if "UMOWA DAROWIZNY" in row.get_text():
                        umowa_darowizny = 1
                        data["Umowa darowizny"] = 1
                    if "POSTANOWIENIE O STWIERDZENIU NABYCIA SPADKU" in row.get_text():
                        nabycie_spadku = 1
                        data["Nabycie spadku"] = 1
                    if "AKT POŚWIADCZENIA DZIEDZICZENIA" in row.get_text():
                        dziedziczenie = 1
                        data["Dziedziczenie"] = 1
                    for element in row.find_all("td"):
                        if "/" in element.get_text():
                            pattern = r"^\d{1,3}/\d{1,3}$"
                            if re.match(pattern, element.get_text().replace(" ", "")):
                                udzial = element.get_text().replace(" ", "")
                            else:
                                udzial = "Brak"

                        # Handle individual person owners
                        if "Osoba fizyczna" in element.get_text():
                            wlasciciel = {}
                            buffer = element.find_next("td").get_text()
                            dane = buffer.split(", ")[0]
                            wlasciciel["kw"] = kw
                            wlasciciel["udzial"] = udzial
                            wlasciciel["czy_firma"] = False
                            if "PESEL" not in element.get_text():
                                wlasciciel["pesel"] = "Brak"
                            else:
                                wlasciciel["pesel"] = buffer.split(", ")[-1]
                            wlasciciel["imie"] = dane.split(" ")[0]
                            if len(dane.split(" ")) >= 3:
                                wlasciciel["nazwisko"] = dane.split(" ")[2].replace(
                                    ",", ""
                                )
                            else:
                                wlasciciel["nazwisko"] = dane.split(" ")[1].replace(
                                    ",", ""
                                )
                            wlasciciele.append(wlasciciel)

                        # Handle company owners
                        elif (
                            "Inna osoba prawna lub jednostka organizacyjna niebędąca osobą prawną"
                            in element.get_text()
                        ):
                            wlasciciel = {}
                            buffer = element.find_next("td").get_text()
                            # Extract company name (first part before comma)
                            nazwa = buffer.split(",")[0].strip()
                            wlasciciel["kw"] = kw
                            wlasciciel["udzial"] = udzial
                            wlasciciel["nazwa"] = nazwa
                            wlasciciel["czy_firma"] = True
                            wlasciciel["imie"] = "Brak"
                            wlasciciel["nazwisko"] = "Brak"
                            wlasciciel["pesel"] = "Brak"
                            wlasciciele.append(wlasciciel)

                        # Handle local government units
                        elif "Jednostka samorządu terytorialnego" in element.get_text():
                            wlasciciel = {}
                            buffer = element.find_next("td").get_text()
                            nazwa = buffer.strip()
                            wlasciciel["kw"] = kw
                            wlasciciel["udzial"] = udzial
                            wlasciciel["nazwa"] = nazwa
                            wlasciciel["czy_firma"] = True
                            wlasciciel["imie"] = "Brak"
                            wlasciciel["nazwisko"] = "Brak"
                            wlasciciel["pesel"] = "Brak"
                            wlasciciele.append(wlasciciel)

                        # Handle State Treasury
                        elif "Skarb Państwa" in element.get_text():
                            wlasciciel = {}
                            buffer = element.find_next("td").get_text()
                            nazwa = buffer.strip()
                            wlasciciel["kw"] = kw
                            wlasciciel["udzial"] = udzial
                            wlasciciel["nazwa"] = nazwa
                            wlasciciel["czy_firma"] = True
                            wlasciciel["imie"] = "Brak"
                            wlasciciel["nazwisko"] = "Brak"
                            wlasciciel["pesel"] = "Brak"
                            wlasciciele.append(wlasciciel)

        # Retrieve mortgage information
        if "DIV.html" in file_path:
            kwota_hipoteka = "Brak"
            if "BRAK WPISÓW" not in str(soup):
                # Mortgage exists
                div_content = soup.find("div", id="contentDzialu")
                if div_content:
                    # Dictionary to store mortgage basis numbers and their details
                    mortgage_entries = {}

                    # First pass: collect all basis numbers for mortgages
                    for table in div_content.find_all("table", class_="tbOdpis"):
                        current_nr_podstawy = None

                        for row in table.find_all("tr"):
                            cells = row.find_all("td")

                            # Look for "Nr podstawy wpisu" header and find basis numbers
                            for i, cell in enumerate(cells):
                                if "Nr podstawy wpisu" in cell.get_text():
                                    # Look for basis numbers in the same row or following rows
                                    for j in range(i + 1, len(cells)):
                                        cell_text = cells[j].get_text().strip()
                                        # Handle multiple basis numbers separated by commas
                                        if re.match(r"^[\d,\s]+$", cell_text):
                                            basis_numbers = [
                                                num.strip()
                                                for num in cell_text.replace(
                                                    " ", ""
                                                ).split(",")
                                                if num.strip().isdigit()
                                            ]
                                            for basis_num in basis_numbers:
                                                mortgage_entries[basis_num] = {
                                                    "details": None
                                                }
                                            break

                            # Check if current row has a basis number in a cell with specific positioning
                            for cell in cells:
                                cell_text = cell.get_text().strip()
                                # Look for standalone numbers or comma-separated numbers
                                if (
                                    re.match(r"^[\d,\s]+$", cell_text)
                                    and len(cell_text) <= 10
                                ):
                                    if cell.get("rowspan") or "csDane" in cell.get(
                                        "class", []
                                    ):
                                        basis_numbers = [
                                            num.strip()
                                            for num in cell_text.replace(" ", "").split(
                                                ","
                                            )
                                            if num.strip().isdigit()
                                        ]
                                        for basis_num in basis_numbers:
                                            if basis_num not in mortgage_entries:
                                                mortgage_entries[basis_num] = {
                                                    "details": None
                                                }

                    # Second pass: find document details for mortgage entries
                    for table in div_content.find_all("table", class_="tbOdpis"):
                        table_text = table.get_text()

                        # Only process tables that contain document basis information
                        if "DOKUMENTY BĘDĄCE PODSTAWĄ WPISU" in table_text:
                            for row in table.find_all("tr"):
                                cells = row.find_all("td")

                                # Look for cells that contain basis numbers
                                for i, cell in enumerate(cells):
                                    cell_text = cell.get_text().strip()

                                    # Check if this cell contains a basis number that we collected
                                    if cell_text in mortgage_entries:
                                        # Find the document details - usually in the next cell
                                        if i + 1 < len(cells):
                                            details_cell = cells[i + 1]
                                            details_text = details_cell.get_text()

                                            # Extract date using regex - look for any YYYY-MM-DD pattern
                                            date_match = re.search(
                                                r"(\d{4}-\d{2}-\d{2})", details_text
                                            )
                                            if date_match:
                                                date_str = date_match.group(1)

                                                hipoteka_entry = {
                                                    "kw": kw,
                                                    "data_wydania": date_str,
                                                }
                                                hipoteki.append(hipoteka_entry)
                                                print(
                                                    f"Found mortgage date: {date_str} for book: {kw}"
                                                )

                    # Extract mortgage amount (existing logic)
                    for row in div_content.find_all("tr"):
                        try:
                            if "Suma" in row.find_all("td")[0].get_text():
                                kwota_hipoteka = format_kwota(
                                    row.find_all("td")[1].get_text()
                                )
                                data["Kwota hipoteki"] = kwota_hipoteka
                        except:
                            pass

        # Retrieve basic information about the land register
        if "DIO.html" in file_path:
            START = False

            div_content = soup.find("div", id="contentDzialu")
            if div_content:
                for row in div_content.find_all("tr"):
                    cells = row.find_all("td")

                    if len(cells) == 0:
                        continue  # Skip if no cells are found

                    td_class = cells[0].get("class", [None])[0]
                    if td_class == "csTTytul":
                        if "DANE O WNIOSKU" in cells[0].get_text():
                            break

                    if typ == "LOKAL STANOWIĄCY ODRĘBNĄ NIERUCHOMOŚĆ":
                        if "Pole powierzchni użytkowej" in row.get_text():
                            powierzchnia = row.find_all("td")
                            if len(powierzchnia) > 1 and powierzchnia[1]:
                                pole_powierzchni = powierzchnia[1].get_text().strip()
                            else:
                                pole_powierzchni = "Brak"
                            data["Pole powierzchni użytkowej"] = pole_powierzchni
                        if "Ulica" in row.get_text():
                            adres = row.find_all("td")
                            ulica = ""
                            numer_budynku = ""
                            numer_lokalu = ""
                            values_table = []
                            for element in adres:
                                values_table.append(element.get_text())

                            values_table = values_table[3:]

                            for idx, element in enumerate(adres):
                                if element.get_text() == "Ulica":
                                    ulica = values_table[0]
                                elif element.get_text() == "Numer budynku":
                                    numer_budynku = values_table[1]
                                elif element.get_text() == "Numer lokalu":
                                    numer_lokalu = values_table[2]
                            pelny_adres = f"{ulica} {numer_budynku}/{numer_lokalu}"
                            if ulica == "":
                                pelny_adres = "Brak"
                            data["Pelny adres"] = pelny_adres
                            data["Ulica"] = ulica

                    if len(cells) > 1 and "csDane" in cells[0].get("class", []):
                        key = (
                            cells[0]
                            .get_text(strip=False)
                            .replace("\xa0", " ")
                            .replace("  ", " ")
                        )

                        try:
                            VALUE_ITER = 1
                            value = (
                                cells[VALUE_ITER]
                                .get_text(strip=True)
                                .replace("\xa0", " ")
                                .replace("  ", " ")
                            )
                            if "Lp." in value:
                                VALUE_ITER += 2
                                value = (
                                    cells[VALUE_ITER]
                                    .get_text(strip=True)
                                    .replace("\xa0", " ")
                                    .replace("  ", " ")
                                )
                        except IndexError:
                            value = (
                                cells[VALUE_ITER - 1]
                                .get_text(strip=True)
                                .replace("\xa0", " ")
                                .replace("  ", " ")
                            )

                        # Check if the cell contains a link, which is Geoportal
                        link = cells[1].find("a")
                        if link:
                            value = link.get_text(strip=True)

                        if START:
                            data[key] = value

                        if "Lp." in key:
                            START = True

        # Update the modified land register and save to a new file
        OUTPUT_HTML = str(soup)
        with open(file_path, "w", encoding="utf-8") as output:
            output.write(OUTPUT_HTML)

        return (
            kw,
            egzekucje,
            hipoteki,
            spadki,
            dziedziczenia,
            darowizny,
        )  # Return all data lists

    def insert_kw(session, kw, data, wlasciciele):
        database_values = {
            "numer": "Brak",
            "identyfikator": "Brak",
            "obreb_ewidencyjny": "Brak",
            "polozenie": "Brak",
            "sposob_korzystania": "Brak",
            "obszar_calej": "Brak",
            "kwota_hipoteki": "Brak",
            "typ": "Brak",
            "ulica": "Brak",
            "adres": "Brak",
            "darowizna": 0,
            "egzekucja": 0,
            "spadek": 0,
            "dziedziczenie": 0,
        }

        for key, value in data.items():
            if key.startswith("Umowa darowizny"):
                database_values["darowizna"] = value
            if key.startswith("Nabycie spadku"):
                database_values["spadek"] = value
            if key.startswith("Dziedziczenie"):
                database_values["dziedziczenie"] = value
            if key.startswith("Wszczęcie egzekucji"):
                database_values["egzekucja"] = value
            if key.startswith("Kwota hipoteki"):
                database_values["kwota_hipoteki"] = value
            if key.startswith("Numer "):
                database_values["numer"] = value
            if key.startswith("Identyfikator "):
                database_values["identyfikator"] = value
            if key.startswith("Obręb ewidencyjny"):
                database_values["obreb_ewidencyjny"] = value
            if key.startswith("Położenie"):
                database_values["polozenie"] = value
            if key.startswith("Sposób korzystania"):
                database_values["sposob_korzystania"] = value
            if key.startswith("Obszar całej nieruchomości"):
                database_values["obszar_calej"] = value
            if key.startswith("Pole powierzchni użytkowej"):
                database_values["obszar_calej"] = value
            if key.startswith("Typ"):
                database_values["typ"] = value
            if key.startswith("Ulica"):
                database_values["ulica"] = value
            if key.startswith("Pelny adres"):
                database_values["adres"] = value

        data.clear()

        # Check if the record already exists
        existing_record = session.query(Informacje).filter_by(ksiega=kw).first()

        # If the record doesn't exist, insert it
        if not existing_record:
            new_informacja = Informacje(**database_values, ksiega=kw)
            session.add(new_informacja)

        else:
            changes_made = False
            for key, value in database_values.items():
                if getattr(existing_record, key) != value:
                    setattr(existing_record, key, value)
                    changes_made = True

            if changes_made:
                session.add(existing_record)

        insert_wl(session, kw, wlasciciele)

    def insert_wl(session, kw, wlasciciele):
        # Fetch all existing owners for the current `kw` to avoid duplicates
        existing_owners = session.query(Wlasciciele).filter_by(ksiega=kw).all()

        # Create a dictionary for fast lookup using a combination of keys
        existing_owners_dict = {
            (
                owner.ksiega,
                owner.pesel,
                owner.imie,
                owner.nazwisko,
                owner.udzial,
                owner.nazwa,
                owner.czy_firma,
            ): owner
            for owner in existing_owners
        }

        for entry in wlasciciele:
            pesel = entry.get("pesel", "Brak")
            imie = entry.get("imie", "Brak")
            nazwisko = entry.get("nazwisko", "Brak")
            udzial = entry.get("udzial", "Brak")
            nazwa = entry.get("nazwa", "Brak")
            # Ensure czy_firma is properly converted to boolean, treating None as False
            czy_firma_raw = entry.get("czy_firma", False)
            czy_firma = bool(czy_firma_raw) if czy_firma_raw is not None else False

            # Create a unique key for each owner entry
            owner_key = (kw, pesel, imie, nazwisko, udzial, nazwa, czy_firma)

            if owner_key in existing_owners_dict:
                existing_owner = existing_owners_dict[owner_key]
                changes_made = False

                for key, value in entry.items():
                    if key == "czy_firma":
                        value = (
                            bool(value) if value is not None else False
                        )  # Treat None as False
                    if (
                        hasattr(existing_owner, key)
                        and getattr(existing_owner, key) != value
                    ):
                        setattr(existing_owner, key, value)
                        changes_made = True

                if changes_made:
                    session.add(existing_owner)

            else:
                new_wlasciciel = Wlasciciele(
                    ksiega=kw,
                    imie=imie,
                    nazwisko=nazwisko,
                    pesel=pesel,
                    udzial=udzial,
                    nazwa=nazwa,
                    czy_firma=czy_firma,  # Already handled None case above
                    wiek=calculate_age(pesel)
                    if pesel != "Brak" and not czy_firma
                    else 0,
                )
                session.add(new_wlasciciel)

        # Clear the list of owners to avoid reprocessing the same data
        wlasciciele.clear()

    def insert_egzekucje(session, kw, egzekucje):
        """Insert execution records into the database"""
        for entry in egzekucje:
            try:
                # Parse the date string
                date_obj = datetime.strptime(entry["data_wszczecia"], "%Y-%m-%d").date()

                # Check if this execution record already exists
                existing_egzekucja = (
                    session.query(Egzekucje)
                    .filter_by(ksiega=kw, data_wszczecia=date_obj)
                    .first()
                )

                if not existing_egzekucja:
                    new_egzekucja = Egzekucje(ksiega=kw, data_wszczecia=date_obj)
                    session.add(new_egzekucja)

            except ValueError as e:
                # Handle invalid date format
                print(f"Invalid date format for ksiega {kw}: {entry['data_wszczecia']}")
                continue

    def insert_hipoteki(session, kw, hipoteki):
        """Insert mortgage records into the database"""
        for entry in hipoteki:
            try:
                # Parse the date string
                date_obj = datetime.strptime(entry["data_wydania"], "%Y-%m-%d").date()

                # Check if this mortgage record already exists
                existing_hipoteka = (
                    session.query(Hipoteki)
                    .filter_by(ksiega=kw, data_wydania=date_obj)
                    .first()
                )

                if not existing_hipoteka:
                    new_hipoteka = Hipoteki(ksiega=kw, data_wydania=date_obj)
                    session.add(new_hipoteka)

            except ValueError as e:
                # Handle invalid date format
                print(f"Invalid date format for ksiega {kw}: {entry['data_wydania']}")
                continue

    def insert_spadki(session, kw, spadki):
        """Insert inheritance records into the database"""
        # Sort by date to get the latest one
        if spadki:
            spadki_sorted = sorted(
                spadki, key=lambda x: x["data_orzeczenia"], reverse=True
            )
            latest_spadek = spadki_sorted[0]  # Get the most recent one

            try:
                # Parse the date string
                date_obj = datetime.strptime(
                    latest_spadek["data_orzeczenia"], "%Y-%m-%d"
                ).date()

                # Check if this inheritance record already exists
                existing_spadek = (
                    session.query(Spadki)
                    .filter_by(ksiega=kw, data_orzeczenia=date_obj)
                    .first()
                )

                if not existing_spadek:
                    new_spadek = Spadki(ksiega=kw, data_orzeczenia=date_obj)
                    session.add(new_spadek)
                    print(f"Found inheritance date: {date_obj} for book: {kw}")

            except ValueError as e:
                # Handle invalid date format
                print(
                    f"Invalid date format for ksiega {kw}: {latest_spadek['data_orzeczenia']}"
                )

    def insert_dziedziczenia(session, kw, dziedziczenia):
        """Insert inheritance by act records into the database"""
        for entry in dziedziczenia:
            try:
                # Parse the date string
                date_obj = datetime.strptime(
                    entry["data_orzeczenia"], "%Y-%m-%d"
                ).date()

                # Check if this inheritance by act record already exists
                existing_dziedziczenie = (
                    session.query(Dziedziczenia)
                    .filter_by(ksiega=kw, data_orzeczenia=date_obj)
                    .first()
                )

                if not existing_dziedziczenie:
                    new_dziedziczenie = Dziedziczenia(
                        ksiega=kw, data_orzeczenia=date_obj
                    )
                    session.add(new_dziedziczenie)

            except ValueError as e:
                # Handle invalid date format
                print(
                    f"Invalid date format for ksiega {kw}: {entry['data_orzeczenia']}"
                )
                continue

    def insert_darowizny(session, kw, darowizny):
        """Insert gift contract records into the database"""
        for entry in darowizny:
            try:
                # Parse the date string
                date_obj = datetime.strptime(entry["data_umowy"], "%Y-%m-%d").date()

                # Check if this gift contract record already exists
                existing_darowizna = (
                    session.query(Darowizny)
                    .filter_by(ksiega=kw, data_umowy=date_obj)
                    .first()
                )

                if not existing_darowizna:
                    new_darowizna = Darowizny(ksiega=kw, data_umowy=date_obj)
                    session.add(new_darowizna)

            except ValueError as e:
                # Handle invalid date format
                print(f"Invalid date format for ksiega {kw}: {entry['data_umowy']}")
                continue

    # Main directory traversal logic
    subdirectory_path = os.path.join(MAIN_DIR, subdirectory_path)
    if not os.path.exists(subdirectory_path):
        return

    # Get files in the subdirectory
    files_in_dir = os.listdir(subdirectory_path)
    # Filter only .html files
    html_files = [f for f in files_in_dir if f.endswith(".html")]
    # Sort files in reverse alphanumeric order
    html_files.sort(reverse=True)

    data = {}
    wlasciciele = []
    egzekucje = []
    hipoteki = []
    spadki = []
    dziedziczenia = []
    darowizny = []
    kw = None

    for file_name in html_files:
        file_path = os.path.join(subdirectory_path, file_name)
        result = process_kw(file_path, data, wlasciciele)
        if isinstance(result, tuple) and len(result) == 6:
            (
                kw,
                file_egzekucje,
                file_hipoteki,
                file_spadki,
                file_dziedziczenia,
                file_darowizny,
            ) = result
            egzekucje.extend(file_egzekucje)
            hipoteki.extend(file_hipoteki)
            spadki.extend(file_spadki)
            dziedziczenia.extend(file_dziedziczenia)
            darowizny.extend(file_darowizny)
        elif isinstance(result, tuple) and len(result) == 3:
            kw, file_egzekucje, file_hipoteki = result
            egzekucje.extend(file_egzekucje)
            hipoteki.extend(file_hipoteki)
        elif isinstance(result, tuple) and len(result) == 2:
            kw, file_egzekucje = result
            egzekucje.extend(file_egzekucje)
        else:
            kw = result

    if kw:
        print(f"[Parser] Inserting KW {kw} into DB...")
        insert_kw(session, kw, data, wlasciciele)
    else:
        print(
            f"[Parser] WARNING: No KW number found in {subdirectory_path}. content might be invalid."
        )

        if egzekucje:
            insert_egzekucje(session, kw, egzekucje)
        if hipoteki:
            insert_hipoteki(session, kw, hipoteki)
        if spadki:
            insert_spadki(session, kw, spadki)
        if dziedziczenia:
            insert_dziedziczenia(session, kw, dziedziczenia)
        if darowizny:
            insert_darowizny(session, kw, darowizny)

    # Close the session after processing
    session.commit()
    session.close()

    if kw:
        return True
    else:
        return False


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        subdirectory = sys.argv[1]
    else:
        subdirectory = input("Podaj nazwę subkatalogu do przetworzenia: ")
    parse_directory(subdirectory)
