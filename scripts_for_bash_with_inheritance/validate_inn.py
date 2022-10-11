from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def calc_company_check_digit(number):
    """Calculate the check digit for the 10-digit ??? for organisations."""
    weights = (2, 4, 10, 3, 5, 9, 4, 6, 8)
    return str(sum(w * int(n) for w, n in zip(weights, number)) % 11 % 10)


def calc_personal_check_digits(number):
    """Calculate the check digits for the 12-digit personal ???."""
    weights = (7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
    d1 = str(sum(w * int(n) for w, n in zip(weights, number)) % 11 % 10)
    weights = (3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
    d2 = str(sum(w * int(n) for w, n in zip(weights, number[:10] + d1)) % 11 % 10)
    return d1 + d2


def validate(number):
    """Check if the number is a valid ???. This checks the length, formatting
    and check digit."""
    number = clean(number, ' ').strip()
    if not isdigits(number):
        raise InvalidFormat()
    if len(number) == 10:
        if calc_company_check_digit(number) != number[-1]:
            raise InvalidChecksum()
    elif len(number) == 12:
        # persons
        if calc_personal_check_digits(number) != number[-2:]:
            raise InvalidChecksum()
    else:
        raise InvalidLength()
    return number


def is_valid(number):
    """Check if the number is a valid ???."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
