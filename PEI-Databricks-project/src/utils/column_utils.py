import re

def clean_column_names(df):

    new_cols = []

    for c in df.columns:

        c = c.strip()
        c = c.lower()

        # replace invalid characters
        c = re.sub(r"[ ,;{}()\n\t=]", "_", c)

        # remove duplicate underscores
        c = re.sub(r"_+", "_", c)

        c = c.strip("_")

        new_cols.append(c)

    return df.toDF(*new_cols)