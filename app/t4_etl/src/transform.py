__author__ = "Fabio Classo"


def transform(df_dct):
    fact = df_dct["fact"]
    lookup = df_dct["lookup"]
    return fact.join(lookup, "pincode")