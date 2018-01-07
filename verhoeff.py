# -*- coding: utf-8 -*-
"""
Verhoeff checksum for last digit of number

@see http://en.wikipedia.org/wiki/Verhoeff_algorithm">More Info
@see http://en.wikipedia.org/wiki/Dihedral_group">Dihedral Group
@see http://mathworld.wolfram.com/DihedralGroupD5.html">Dihedral Group Order 10
@author Hermann Himmelbauer
"""

verhoeff_table_d = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 2, 3, 4, 0, 6, 7, 8, 9, 5),
    (2, 3, 4, 0, 1, 7, 8, 9, 5, 6),
    (3, 4, 0, 1, 2, 8, 9, 5, 6, 7),
    (4, 0, 1, 2, 3, 9, 5, 6, 7, 8),
    (5, 9, 8, 7, 6, 0, 4, 3, 2, 1),
    (6, 5, 9, 8, 7, 1, 0, 4, 3, 2),
    (7, 6, 5, 9, 8, 2, 1, 0, 4, 3),
    (8, 7, 6, 5, 9, 3, 2, 1, 0, 4),
    (9, 8, 7, 6, 5, 4, 3, 2, 1, 0))
verhoeff_table_p = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 5, 7, 6, 2, 8, 3, 0, 9, 4),
    (5, 8, 0, 3, 7, 9, 6, 1, 4, 2),
    (8, 9, 1, 6, 0, 4, 3, 5, 2, 7),
    (9, 4, 5, 3, 1, 2, 6, 8, 7, 0),
    (4, 2, 8, 6, 5, 7, 3, 9, 0, 1),
    (2, 7, 9, 3, 8, 0, 6, 4, 1, 5),
    (7, 0, 4, 6, 9, 1, 3, 2, 5, 8))
verhoeff_table_inv = (0, 4, 3, 2, 1, 5, 6, 7, 8, 9)


def is_number(s):
    '''Check if is a number.  Return True if yes, False if no.'''
    try:
        int(s)
        return True
    except ValueError:
        return False


def calcsum(number):
    '''For a given number returns a Verhoeff checksum digit'''
    try:
        c = 0
        for i, item in enumerate(reversed(str(number))):
            c = verhoeff_table_d[c][verhoeff_table_p[(i+1) % 8][int(item)]]
        return verhoeff_table_inv[c]
    except ValueError:
        return 99


def checksum(number):
    '''For a given number generates a Verhoeff digit and
    returns number + digit'''
    try:
        c = 0
        for i, item in enumerate(reversed(str(number))):
            c = verhoeff_table_d[c][verhoeff_table_p[i % 8][int(item)]]
        return c
    except ValueError:
        return 99


def generateVerhoeff(number):
    '''For a given number returns number + Verhoeff checksum digit'''
    return "%s%s" % (number, calcsum(number))


def validateVerhoeff(number):
    '''Validate Verhoeff checksummed number (checksum is last digit)'''
    return checksum(number) == 0

# Some tests and also usage examples
assert calcsum('75872') == 2
assert checksum('758722') == 0
assert calcsum('12345') == 1
assert checksum('123451') == 0
assert calcsum('142857') == 0
assert checksum('1428570') == 0
assert calcsum('123456789012') == 0
assert checksum('1234567890120') == 0
assert calcsum('8473643095483728456789') == 2
assert checksum('84736430954837284567892') == 0
assert generateVerhoeff('12345') == '123451'
assert validateVerhoeff('123451') is True
assert validateVerhoeff('122451') is False
assert validateVerhoeff('128451') is False
