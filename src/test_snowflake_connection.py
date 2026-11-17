
import getpass
import snowflake.connector


def main():
    print("Conexión a Snowflake")
    password = getpass.getpass("Introduce tu contraseña de Snowflake: ")

    connection = snowflake.connector.connect(
        account="YU12182.us-east-2.aws",
        user="JOHINER17",
        password=password,
        authenticator="snowflake",
        warehouse="WH_TAXI_ETL",
        database="NYC_TAXI_DW",
        schema="RAW",
        role="TAXI_ETL_ROLE",
    )

    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT
                CURRENT_USER(),
                CURRENT_ACCOUNT(),
                CURRENT_REGION(),
                CURRENT_ROLE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_WAREHOUSE()
        """)

        resultado = cursor.fetchone()

        print("\nConexión establecida correctamente.")
        print(f"Usuario: {resultado[0]}")
        print(f"Cuenta: {resultado[1]}")
        print(f"Región: {resultado[2]}")
        print(f"Rol: {resultado[3]}")
        print(f"Base de datos: {resultado[4]}")
        print(f"Esquema: {resultado[5]}")
        print(f"Warehouse: {resultado[6]}")

        cursor.close()

    finally:
        connection.close()


if __name__ == "__main__":
    main()
