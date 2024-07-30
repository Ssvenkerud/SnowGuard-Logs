"""Utility functions for Azure Key Vault and other Azure servicesi"""

def stringcleanup(string):
    """Remove unwanted characters from a string.

    character selection ois based on characters found in acutal queries.
    """
    string = string.replace('"', "")
    string = string.replace("'", "")
    string = string.replace(";", "")
    string = string.replace("(", "")
    string = string.replace(")", "")
    string = string.replace("!", "")
    string = string.replace("@", "")
    string = string.replace("IDENTIFIER", "")
    return string
