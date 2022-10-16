class SqlQueries:
    
    Immigrante_table_insert = (
        """
        INSERT INTO Immigrante_table(cicid,adnum, i94cit, i94port, arrdate, dedate)
        SELECT distinct cicid,adnum, i94cit, i94port, arrdate, dedate 
        FROM staging_Immigrante
    """)

    person_table_insert = (
        """
        INSERT INTO person_table_dim(i94res, i94port, i94mode, i94cit, i94mon, arrdate, i94addr, dedate,
        i94bir, i94visa, occup, biryear, gender, airline, adnum,  fltno)
        SELECT distinct i94res, i94port, i94mode, i94cit, i94mon, arrdate, i94addr, dedate,
        i94bir, i94visa, occup, biryear, gender, airline,  adnum, fltno
        FROM staging_person
    """)

    airport_table_insert = (
        """
        INSERT INTO airport_table_dim(ID, type, name, iso_country, Municipality, iata_code, local_code)
        SELECT distinct ID, type, name, iso_country, Municipality, iata_code, local_code
        FROM staging_airport
    """)
    
    demographic_table_insert = (
        """
        INSERT INTO demographic_table_dim(city,state,male_population,female_population,
        total_male_population,Number_of_ventreans,foreign_born,state_code,Average_Household_Size,race,Count)
        SELECT distinct city, state, male_population, female_population, total_male_population, Number_of_ventreans,
        foreign_born,state_code,Average_Household_Size,race,Count
        FROM staging_demographic
    """)