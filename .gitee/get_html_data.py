import os
from lxml import etree
import argparse

def get_llt(dir_path):

    html = etree.parse(dir_path, etree.HTMLParser())
    text3 = html.xpath("/html/body/table[1]/tr[3]/td/table/tr[2]/td[7]/text()")
    s = text3[0].split(" ")[0]
    return s


if __name__ == "__main__":
    args = argparse.ArgumentParser(description="Flag for test.")
    args.add_argument("--dir_path", type=str, help="html dir.")

    args = args.parse_args()
    print(get_llt(args.dir_path))