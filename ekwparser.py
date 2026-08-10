import os
import re
import sys
from datetime import datetime

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from www.models import Informacje, Wlasciciele

load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Create the tables if they don't exist
Base.metadata.create_all(engine)


def parse_whole_directory(prefix="", **kwargs):
    # Initialize session
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
                for row in div_content.find_all("tr"):
                    if "WSZCZĘCIU EGZEKUCJI" in row.get_text():
                        wszczecie_egzekucji = 1
                        data["Wszczęcie egzekucji"] = 1

        # Retrieve owner information
        if "DII.html" in file_path:
            div_content = soup.find("div", id="contentDzialu")
            if div_content:
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
                        if "Osoba fizyczna" in element.get_text():
                            wlasciciel = {}
                            buffer = element.find_next("td").get_text()
                            dane = buffer.split(", ")[0]
                            wlasciciel["kw"] = kw
                            wlasciciel["udzial"] = udzial
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

        # Retrieve mortgage information
        if "DIV.html" in file_path:
            kwota_hipoteka = "Brak"
            if "BRAK WPISÓW" not in str(soup):
                # Mortgage exists
                div_content = soup.find("div", id="contentDzialu")
                if div_content:
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

        return kw

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

        # Create a dictionary for fast lookup using a combination of keys like (ksiega, pesel, imie, nazwisko, udzial)
        existing_owners_dict = {
            (owner.ksiega, owner.pesel, owner.imie, owner.nazwisko, owner.udzial): owner
            for owner in existing_owners
        }

        for entry in wlasciciele:
            pesel = entry.get("pesel", "Brak")
            imie = entry.get("imie", "Brak")
            nazwisko = entry.get("nazwisko", "Brak")
            udzial = entry.get("udzial", "Brak")

            # Create a unique key for each owner entry
            owner_key = (kw, pesel, imie, nazwisko, udzial)

            if owner_key in existing_owners_dict:
                existing_owner = existing_owners_dict[owner_key]
                changes_made = False

                for key, value in entry.items():
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
                    wiek=calculate_age(pesel) if pesel != "Brak" else 0,
                )
                session.add(new_wlasciciel)

        # Clear the list of owners to avoid reprocessing the same data
        wlasciciele.clear()

    # Main directory traversal logic

    for root, dirs, files in os.walk(MAIN_DIR):
        dirs.sort(reverse=True)
        for dir_name in dirs:
            if dir_name.startswith(prefix):
                folder_path = os.path.join(root, dir_name)
                # Get files in the folder
                files_in_dir = os.listdir(folder_path)
                # Filter only .html files
                html_files = [f for f in files_in_dir if f.endswith(".html")]
                # Sort files in reverse alphanumeric order
                html_files.sort(reverse=True)

                data = {}
                wlasciciele = []
                kw = None

                for file_name in html_files:
                    file_path = os.path.join(folder_path, file_name)
                    kw = process_kw(file_path, data, wlasciciele)

                if kw:
                    insert_kw(session, kw, data, wlasciciele)

    # Close the session after processing
    session.commit()
    session.close()


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        prefix = sys.argv[1]
    else:
        prefix = ""
    parse_whole_directory(prefix)
