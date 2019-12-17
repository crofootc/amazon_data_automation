import mysql.connector
conn = mysql.connector.connect(host="localhost", user="root", database="vooray", password="mis5900vooray")

curs = conn.cursor()
print(curs)

def main():
    curs.execute(
        f'''
        DROP TABLE `vooray_c`.`amz_price`, `vooray_c`.`inv_level`, `vooray_c`.`item`, `vooray_c`.`quantity_sold`;
        ''')

    print("DATABASE DROPPED")


main()

