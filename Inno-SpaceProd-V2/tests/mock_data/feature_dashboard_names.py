from pyspark import Row

from tests.utils.spark_util import spark_tests as spark

DF_FEATURE_DASHBOARD_NAMES = spark.sparkContext.parallelize(
    [
        Row(
            "2021_All_Other_Visible_Minorities_10",
            "Other Visible Minorities",
            "Population",
        ),
        Row("2021_Average_Age_Of_Total_Population_10", "Average Age", "Age"),
        Row(
            "2021_Average_Children_Per_Household_10",
            "Average Children per Household",
            "Children",
        ),
        Row(
            "2021_Average_Household_Income_Current_Year__10",
            "Average Household Income",
            "Income",
        ),
        Row(
            "2021_Household_Income_0_To_19999_Current_Year__10",
            "Household Income $0K-$20K",
            "Income",
        ),
        Row(
            "2021_Household_Income_100000_Or_Over_Current_Year__10",
            "Household Income $100K+",
            "Income",
        ),
        Row(
            "2021_Household_Income_100000_To_124999_Current_Year__10",
            "Household Income $100K-$125K",
            "Income",
        ),
        Row(
            "2021_Household_Income_125000_To_149999_Current_Year__10",
            "Household Income $125K-$150K",
            "Income",
        ),
        Row(
            "2021_Household_Income_150000_To_199999_Current_Year__10",
            "Household Income $150K-$200K",
            "Income",
        ),
        Row(
            "2021_Household_Income_20000_To_39999_Current_Year__10",
            "Household Income $20K-$40K",
            "Income",
        ),
        Row(
            "2021_Household_Income_200000_Or_Over_Current_Year__10",
            "Household Income $200K+",
            "Income",
        ),
        Row(
            "2021_Household_Income_200000_To_299999_Current_Year__10",
            "Household Income $200K-$300K",
            "Income",
        ),
        Row(
            "2021_Household_Income_300000_Or_Over_Current_Year__10",
            "Household Income $300K+",
            "Income",
        ),
        Row(
            "2021_Household_Income_40000_To_59999_Current_Year__10",
            "Household Income $40K-$60K",
            "Income",
        ),
        Row(
            "2021_Household_Income_60000_To_79999_Current_Year__10",
            "Household Income $60K-$80K",
            "Income",
        ),
        Row(
            "2021_Household_Income_80000_To_99999_Current_Year__10",
            "Household Income $80K-$100K",
            "Income",
        ),
        Row(
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_10",
            "Below Bachelors",
            "Education",
        ),
        Row(
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_Above_Bachelors_10",
            "Above Bachelors",
            "Education",
        ),
        Row(
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_Apprenticeship_Or_Trades_Certificate_Or_Diploma_10",
            "Apprenticeship or Trades Certificate",
            "Education",
        ),
        Row(
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_Bachelors_Degree_10",
            "Bacheelors Degree",
            "Education",
        ),
        Row(
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_College_Cegep_Or_Other_Nonuniversity_Certificate_Or_Diploma_10",
            "College, Cegep, Other Non-University",
            "Education",
        ),
        Row(
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_High_School_Certificate_Or_Equivalent_10",
            "High School Certificate",
            "Education",
        ),
        Row(
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_No_Certificate_Diploma_Or_Degree_10",
            "No Diploma or Degree",
            "Education",
        ),
        Row(
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_University_Certificate_Or_Diploma_Below_Bachelor_10",
            "University Degree or Diploma",
            "Education",
        ),
        Row(
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_University_Degree_10",
            "University Degree",
            "Education",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Afghanistan_10",
            "Immigrant Population - Afghanistan",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Africa_10",
            "Immigrant Population - Africa",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Algeria_10",
            "Immigrant Population - Algeria",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Americas_10",
            "Immigrant Population - Americas",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Asia_10",
            "Immigrant Population - Asia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Australia_10",
            "Immigrant Population - Australia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Bangladesh_10",
            "Immigrant Population - Bangladesh",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Bosnia_Herzegovina_10",
            "Immigrant Population - Bosnia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Brazil_10",
            "Immigrant Population - Brazil",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Cambodia_10",
            "Immigrant Population - Cambodia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Cameroon_10",
            "Immigrant Population - Cameroon",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Caribbean_And_Bermuda_10",
            "Immigrant Population - Bermuda",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Central_Africa_10",
            "Immigrant Population - Central Africa",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Central_America_10",
            "Immigrant Population - Central America",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Chile_10",
            "Immigrant Population - Chile",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_China_10",
            "Immigrant Population - China",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Colombia_10",
            "Immigrant Population - Colombia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Congo_The_Democratic_Republic_Of_The_10",
            "Immigrant Population - Congo",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Croatia_10",
            "Immigrant Population - Croatia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Cte_Divoire_10",
            "Immigrant Population - Cte Divoire",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Cuba_10",
            "Immigrant Population - Cuba",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Czech_Republic_10",
            "Immigrant Population - Czech Republic",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Eastern_Africa_10",
            "Immigrant Population - Eastern Africa",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Eastern_Asia_10",
            "Immigrant Population - Eastern Asia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Eastern_Europe_10",
            "Immigrant Population - Eastern Europe",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Egypt_10",
            "Immigrant Population - Egypt",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_El_Salvador_10",
            "Immigrant Population - El Salvador",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Eritrea_10",
            "Immigrant Population - Eritrea",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Ethiopia_10",
            "Immigrant Population - Ethiopia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Europe_10",
            "Immigrant Population - Europe",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Fiji_10",
            "Immigrant Population - Fiji",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_France_10",
            "Immigrant Population - France",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Germany_10",
            "Immigrant Population - Germany",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Ghana_10",
            "Immigrant Population - Ghana",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Greece_10",
            "Immigrant Population - Greece",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Guyana_10",
            "Immigrant Population - Guyana",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Haiti_10",
            "Immigrant Population - Haiti",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Hong_Kong_10",
            "Immigrant Population - Hong Kong",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Hungary_10",
            "Immigrant Population - Hungary",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_India_10",
            "Immigrant Population - India",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Iran_10",
            "Immigrant Population - Iran",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Iraq_10",
            "Immigrant Population - Iraq",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Ireland_10",
            "Immigrant Population - Ireland",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Israel_10",
            "Immigrant Population - Israel",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Italy_10",
            "Immigrant Population - Italy",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Jamaica_10",
            "Immigrant Population - Jamaica",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Japan_10",
            "Immigrant Population - Japan",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Kenya_10",
            "Immigrant Population - Kenya",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Lebanon_10",
            "Immigrant Population - Lebanon",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Malaysia_10",
            "Immigrant Population - Malaysia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Mexico_10",
            "Immigrant Population - Mexico",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Moldova_10",
            "Immigrant Population - Moldova",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Morocco_10",
            "Immigrant Population - Morocco",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Nepal_10",
            "Immigrant Population - Nepal",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Netherlands_10",
            "Immigrant Population - Netherlands",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Nigeria_10",
            "Immigrant Population - Nigeria",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Nonimmigrant_10",
            "Non Immigrant",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Nonimmigrant_In_Province_Of_Birth_10",
            "Non Immigrant In Province Of Birth",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Nonimmigrant_Outside_Province_Of_Birth_10",
            "Non Immigrant Outside Province Of Birth",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Nonpermanent_Residents_10",
            "Nonpermanent Residents",
            "Population",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_North_America_10",
            "Immigrant Population - North America",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Northern_Africa_10",
            "Immigrant Population - Northern Africa",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Northern_Europe_10",
            "Immigrant Population - Northern Europe",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Pakistan_10",
            "Immigrant Population - Pakistan",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Peru_10",
            "Immigrant Population - Peru",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Philippines_10",
            "Immigrant Population - Philippines",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Poland_10",
            "Immigrant Population - Poland",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Portugal_10",
            "Immigrant Population - Portugal",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Romania_10",
            "Immigrant Population - Romania",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Russia_10",
            "Immigrant Population - Russia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Saudi_Arabia_10",
            "Immigrant Population - Saudi Arabia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Serbia_10",
            "Immigrant Population - Serbia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Somalia_10",
            "Immigrant Population - Somalia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_South_Africa_10",
            "Immigrant Population - South Africa",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_South_America_10",
            "Immigrant Population - South America",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_South_Korea_10",
            "Immigrant Population - South Korea",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Southeastern_Asia_10",
            "Immigrant Population - Southeastern Asia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Southern_Africa_10",
            "Immigrant Population - Southren Africa",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Southern_Asia_10",
            "Immigrant Population - Southern Asia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Southern_Europe_10",
            "Immigrant Population - South Europe",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Sri_Lanka_10",
            "Immigrant Population - Sri Lanka",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Syria_10",
            "Immigrant Population - Syria",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Taiwan_10",
            "Immigrant Population - Taiwan",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Tanzania_10",
            "Immigrant Population - Tanzania",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Total_Immigrant_10",
            "Total Immigrant Population",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Trinidad_And_Tobago_10",
            "Immigrant Population - Trinidad and Tobago",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Tunisia_10",
            "Immigrant Population - Tunisia",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Turkey_10",
            "Immigrant Population - Turkey",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Ukraine_10",
            "Immigrant Population - Ukraine",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_United_Arab_Emirates_10",
            "Immigrant Population - UAE",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_United_Kingdom_10",
            "Immigrant Population - UK ",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_United_States_10",
            "Immigrant Population - USA",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Venezuela_10",
            "Immigrant Population - Venezuela",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Vietnam_10",
            "Immigrant Population - Vietnam",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_West_Central_Asia_And_Middle_East_10",
            "Immigrant Population - Middle East",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Western_Africa_10",
            "Immigrant Population - Western Africa",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Total_Immigration_By_Place_Of_Birth_Western_Europe_10",
            "Immigrant Population - Western Europe",
            "Immigration",
        ),
        Row(
            "2021_Household_Population_For_Visible_Minority_10",
            "Immigrant Population - Visible Minority",
            "Immigration",
        ),
        Row(
            "2021_Household_Size_Average_Number_Of_Persons_In_Private_Households_10",
            "People in Private Households",
            "Population",
        ),
        Row("2021_In_The_Labour_Force_10", "Population in Labour Force", "Population"),
        Row(
            "2021_Median_Household_Income_Current_Year__10",
            "Median Household Income",
            "Income",
        ),
        Row(
            "2021_Multiple_Visible_Minorities_10",
            "Visible Minority - Multiple",
            "Minorities",
        ),
        Row("2021_Not_A_Visible_Minority_10", "Not a Visible Minority", "Minorities"),
        Row(
            "2021_Total_Census_Family_Households_10",
            "Total Family Households",
            "Population",
        ),
        Row(
            "2021_Total_Households_For_Household_Size_10",
            "Total Households",
            "Population",
        ),
        Row(
            "2021_Total_Number_Of_Children_At_Home_10",
            "Total Children at Home",
            "Population",
        ),
        Row("2021_Total_Population_0_To_4_10", "Population Age 0-4", "Age"),
        Row("2021_Total_Population_10", "Population", "Age"),
        Row("2021_Total_Population_10_To_14_10", "Population Age 10-14", "Age"),
        Row("2021_Total_Population_15_To_19_10", "Population Age 15-19", "Age"),
        Row("2021_Total_Population_20_To_24_10", "Population Age 20-24", "Age"),
        Row("2021_Total_Population_25_To_29_10", "Population Age 25-29", "Age"),
        Row("2021_Total_Population_30_To_34_10", "Population Age 30-34", "Age"),
        Row("2021_Total_Population_35_To_39_10", "Population Age 35-39", "Age"),
        Row("2021_Total_Population_40_To_44_10", "Population Age 40-44", "Age"),
        Row("2021_Total_Population_45_To_49_10", "Population Age 45-49", "Age"),
        Row("2021_Total_Population_5_To_9_10", "Population Age 5-9", "Age"),
        Row("2021_Total_Population_50_To_54_10", "Population Age 50-54", "Age"),
        Row("2021_Total_Population_55_To_59_10", "Population Age 55-59", "Age"),
        Row("2021_Total_Population_60_To_64_10", "Population Age 60-64", "Age"),
        Row("2021_Total_Population_65_To_69_10", "Population Age 65-69", "Age"),
        Row("2021_Total_Population_70_To_74_10", "Population Age 70-74", "Age"),
        Row("2021_Total_Population_75_To_79_10", "Population Age 75-79", "Age"),
        Row("2021_Total_Population_80_To_84_10", "Population Age 80-84", "Age"),
        Row("2021_Total_Population_85_Or_Older_10", "Population Age 85+", "Age"),
        Row(
            "2021_Total_Visible_Minorities_10",
            "Visible Minority - Multiple",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_Arab_10",
            "Visible Minority - Arab",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_Black_10",
            "Visible Minority - Black",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_Chinese_10",
            "Visible Minority - Chinese",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_Filipino_10",
            "Visible Minority - Filipino",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_Japanese_10",
            "Visible Minority - Japanese",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_Korean_10",
            "Visible Minority - Korean",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_Latin_American_10",
            "Visible Minority - Latin American",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_South_Asian_10",
            "Visible Minority - South Asian",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_Southeast_Asian_10",
            "Visible Minority - South East Asian",
            "Minorities",
        ),
        Row(
            "2021_Total_Visible_Minorities_West_Asian_10",
            "Visible Minority - West Asian",
            "Minorities",
        ),
    ]
).toDF(["var_details", "EC Features", "Group"])
