def getLetterValue(letter):
    letterValues = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'X',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
        'K', 'L', 'M', 'N', 'O', 'P', 'R', 'S', 'T', 'U',
        'W', 'Y', 'Z'
    ]
    for j in range(len(letterValues)):
        if letter == letterValues[j]:
            return j
    return -1

def validate_kw(kw):
    if kw is None or len(kw) != 15:
        return False

    kw = kw.upper()

    if kw[4] != '/' or kw[13] != '/':
        return False

    for i in range(2):
        if getLetterValue(kw[i]) < 10:
            return False

    if getLetterValue(kw[2]) < 0 or getLetterValue(kw[2]) > 9:
        return False

    if getLetterValue(kw[3]) < 10:
        return False

    for i in range(5, 13):
        if getLetterValue(kw[i]) < 0 or getLetterValue(kw[i]) > 9:
            return False

    sum = 1 * getLetterValue(kw[0]) + \
          3 * getLetterValue(kw[1]) + \
          7 * getLetterValue(kw[2]) + \
          1 * getLetterValue(kw[3]) + \
          3 * getLetterValue(kw[5]) + \
          7 * getLetterValue(kw[6]) + \
          1 * getLetterValue(kw[7]) + \
          3 * getLetterValue(kw[8]) + \
          7 * getLetterValue(kw[9]) + \
          1 * getLetterValue(kw[10]) + \
          3 * getLetterValue(kw[11]) + \
          7 * getLetterValue(kw[12])
    sum %= 10

    if kw[14] != str(sum):
        return False

    return True

def get_control_digit(kod_wydzialu: str, numer_ksiegi: str) -> int:
    """Sprawdzamy cyfrę kontrolną dla danej księgi wieczystej 

    Args:
        kod_wydzialu (str): Kod wydziału
        numer_ksiegi (str): Numer księgi wieczystej

    Returns:
        int: Cyfra kontrolna
    """    
    for i in range(0, 10):
        buffer = f"{kod_wydzialu}/{numer_ksiegi}/{i}"
        if validate_kw(buffer) is True:
            return i

def get_formatted_book_number(numer_ksiegi: str) -> str:
    """Wygeneruj sformatowany numer księgi, który będzie pasować do formatu XXXXXXXX

    Args:
        numer_ksiegi (str): Numer księgi wieczystej
        
    Returns:
        str: Sformatowany numer księgi wieczystej
    """
    width = 8
    formatted_number = numer_ksiegi.zfill(width)
    return formatted_number