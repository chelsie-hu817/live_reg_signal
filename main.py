import config
from data_processing import *


# Define main execution function
def main():
    """Main script execution."""

    # Step 1: Process data
    coso = get_all_data(project_coso, gen_db_coso, start_date, end_date, device_coso, tag_coso, project_id_coso)

    saticoy = get_all_data(project_saticoy, gen_db_saticoy, start_date, end_date, device_saticoy, tag_saticoy, project_id_saticoy)
    condor = get_all_data(project_condor, gen_db_condor, start_date, end_date, device_condor, tag_condor, project_id_condor)
    print("Processing complete.")

    # Step 2: Caculate ru and rd signals
    coso["ru_signal"] = coso.apply(calculate_ru_signal, axis=1)
    coso["rd_signal"] = coso.apply(calculate_rd_signal, axis=1)
    coso.to_excel(config.path_coso, index=False)

    saticoy["ru_signal"] = saticoy.apply(calculate_ru_signal, axis=1)
    saticoy["rd_signal"] = saticoy.apply(calculate_rd_signal, axis=1)
    saticoy.to_excel(config.path_saticoy, index=False)

    condor["ru_signal"] = condor.apply(calculate_ru_signal, axis=1)
    condor["rd_signal"] = condor.apply(calculate_rd_signal, axis=1)
    condor.to_excel(config.path_condor, index=False)


# Standard execution guard
if __name__ == "__main__":
    main()