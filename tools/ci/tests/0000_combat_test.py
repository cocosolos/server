import time


def test_name():
    return "Combat"


def execute_test(cur, db, hxi_client):
    try:
        cur.execute(
            "UPDATE chars SET \
            pos_zone = 174, \
            pos_prevzone = 174, \
            pos_x = 55, \
            pos_y = -9, \
            pos_z = -140 \
        WHERE charid = 1;"
        )

        cur.execute(
            "SELECT charid, accid, charname, pos_zone, pos_x, pos_y, pos_z FROM chars;"
        )
        # Set GodMode CharVar = 1
        cur.execute(
            "INSERT INTO char_vars(charid, varname, value) VALUES(1, 'GodMode', 1);"
        )

        hxi_client.login()
        print("Sleeping 60s")
        time.sleep(60)
        hxi_client.logout()

        return True

    except Exception as e:
        return False
