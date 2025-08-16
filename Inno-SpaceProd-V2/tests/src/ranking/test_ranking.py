import pandas as pd
import pytest
from tests.utils.spark_util import spark_tests as spark

from pyspark import Row
from spaceprod.src.ranking.helpers import *
from spaceprod.utils.imports import SparkDataFrame


@pytest.fixture(scope="session")
def mock_bay_data_prefix() -> SparkDataFrame:
    mock_bay_data_prefix_data = spark.createDataFrame(
        [
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="ontario",
                SECTION_MASTER="Frozen Handhelds",
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                STORE_PHYSICAL_LOCATION_NO="12083",
                ITEM_NO="314792",
                ITEM_NAME="DrOetker Ristornt T/Crst PzVeg",
                Sales=6.28000020980835,
                Facings="1",
                E2E_MARGIN_TOTAL=-0.6384810400555674,
                cannib_id="CANNIB-930",
                need_state="-1",
                Item_Count=2.0,
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="ontario",
                SECTION_MASTER="Frozen Handhelds",
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                STORE_PHYSICAL_LOCATION_NO="12083",
                ITEM_NO="375039",
                ITEM_NAME="DrOetker Glusep Thin Can  510g",
                Sales=83.8599967956543,
                Facings="1",
                E2E_MARGIN_TOTAL=20.63535798492478,
                cannib_id="CANNIB-930",
                need_state="0",
                Item_Count=14.0,
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="ontario",
                SECTION_MASTER="Frozen Pizza",
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                STORE_PHYSICAL_LOCATION_NO="12083",
                ITEM_NO="375030",
                ITEM_NAME="DrOetker Pizza RiCrust 4Cheese",
                Sales=5.989999771118164,
                Facings="1",
                E2E_MARGIN_TOTAL=1.172282917694476,
                cannib_id="CANNIB-930",
                need_state="1",
                Item_Count=1.0,
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="ontario",
                SECTION_MASTER="Frozen Pizza",
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                STORE_PHYSICAL_LOCATION_NO="12083",
                ITEM_NO="678984",
                ITEM_NAME="SMI Opsite 24632 Dr 5x5in  1ea",
                Sales=3.990000009536743,
                Facings="1",
                E2E_MARGIN_TOTAL=1.318333246310552,
                cannib_id="CANNIB-930",
                need_state="-1",
                Item_Count=1.0,
                MERGED_CLUSTER="1B",
            ),
            Row(
                NATIONAL_BANNER_DESC="SOBEYS",
                REGION="ontario",
                SECTION_MASTER="Frozen Handhelds",
                EXEC_ID="ontario_SOBEYS_FrozenHandhelds",
                STORE_PHYSICAL_LOCATION_NO="12083",
                ITEM_NO="900100",
                ITEM_NAME="DrOetker Glusep Thin Can  510g",
                Sales=53.90999794006348,
                Facings="1",
                E2E_MARGIN_TOTAL=13.387525557868386,
                cannib_id="CANNIB-930",
                need_state="0",
                Item_Count=9.0,
                MERGED_CLUSTER="2B",
            ),
        ]
    )

    return mock_bay_data_prefix_data


@pytest.fixture(scope="session")
def mock_optimization_cluster() -> SparkDataFrame:
    mock_optimization_cluster_data = spark.createDataFrame(
        [
            Row(
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                item_no="314792",
                store_physical_location_id="12083",
                need_state_idx=1,
                need_state=-1,
                item_name="DrOetker Pizza Pepperoni Pesto",
                lvl4_name="Frozen Pizza",
                width=10.26,
                constant_treatment=False,
                current_facings=1.0,
                cur_sales=0.502874493598938,
                opt_sales=1.005748987197876,
                optim_facings=3.0,
                unique_items_cur_assorted=1,
                unique_items_opt_assorted=1,
                cur_width_x_facing=10.26,
                opt_width_x_facing=30.78,
                cur_opt_same_facing=0,
                opt_minus_cur_facings=2.0,
                department="Frozen",
                opt_margin=0.1684080809354782,
                cur_margin=0.0842040404677391,
                department_size=16.0,
                size_uom="D",
                store_cluster_id="ONT-SBY-FRZ-1A-16D",
                Item_Count=1,
            ),
            Row(
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                item_no="375039",
                store_physical_location_id="12083",
                need_state_idx=3,
                need_state=1,
                item_name="Delissio RsnC Peprn 12IN",
                lvl4_name="Frozen Pizza",
                width=11.6,
                constant_treatment=False,
                current_facings=1.0,
                cur_sales=0.5495210886001587,
                opt_sales=1.0990421772003174,
                optim_facings=3.0,
                unique_items_cur_assorted=1,
                unique_items_opt_assorted=1,
                cur_width_x_facing=11.6,
                opt_width_x_facing=34.8,
                cur_opt_same_facing=0,
                opt_minus_cur_facings=2.0,
                department="Frozen",
                opt_margin=0.27152788639068604,
                cur_margin=0.13576394319534302,
                department_size=16.0,
                size_uom="D",
                store_cluster_id="ONT-SBY-FRZ-1A-16D",
                Item_Count=1,
            ),
            Row(
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                item_no="375030",
                store_physical_location_id="12083",
                need_state_idx=3,
                need_state=1,
                item_name="Delisso Pzria Deluxe",
                lvl4_name="Frozen Pizza",
                width=12.05,
                constant_treatment=False,
                current_facings=1.0,
                cur_sales=0.5495210886001587,
                opt_sales=0.5495210886001587,
                optim_facings=1.0,
                unique_items_cur_assorted=1,
                unique_items_opt_assorted=1,
                cur_width_x_facing=12.05,
                opt_width_x_facing=12.05,
                cur_opt_same_facing=1,
                opt_minus_cur_facings=0.0,
                department="Frozen",
                opt_margin=0.13576394319534302,
                cur_margin=0.13576394319534302,
                department_size=16.0,
                size_uom="D",
                store_cluster_id="ONT-SBY-FRZ-1A-16D",
                Item_Count=1,
            ),
            Row(
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                item_no="678984",
                store_physical_location_id="12083",
                need_state_idx=2,
                need_state=2,
                item_name="DrOetker Pza CasaDMa ClsscCdn",
                lvl4_name="Frozen Pizza",
                width=10.26,
                constant_treatment=False,
                current_facings=1.0,
                cur_sales=0.5744441747665405,
                opt_sales=0.9104724526405334,
                optim_facings=2.0,
                unique_items_cur_assorted=1,
                unique_items_opt_assorted=1,
                cur_width_x_facing=10.26,
                opt_width_x_facing=20.52,
                cur_opt_same_facing=0,
                opt_minus_cur_facings=1.0,
                department="Frozen",
                opt_margin=0.1590249091386795,
                cur_margin=0.1003335490822792,
                department_size=16.0,
                size_uom="D",
                store_cluster_id="ONT-SBY-FRZ-1A-16D",
                Item_Count=1,
            ),
            Row(
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                item_no="900100",
                store_physical_location_id="12083",
                need_state_idx=1,
                need_state=-1,
                item_name="SMI Opsite 24632 Dr 5x5in  1ea",
                lvl4_name="Frozen Pizza",
                width=11.1,
                constant_treatment=False,
                current_facings=1.0,
                cur_sales=0.502874493598938,
                opt_sales=0.502874493598938,
                optim_facings=1.0,
                unique_items_cur_assorted=1,
                unique_items_opt_assorted=1,
                cur_width_x_facing=11.1,
                opt_width_x_facing=11.1,
                cur_opt_same_facing=1,
                opt_minus_cur_facings=0.0,
                department="Frozen",
                opt_margin=0.0842040404677391,
                cur_margin=0.0842040404677391,
                department_size=16.0,
                size_uom="D",
                store_cluster_id="ONT-SBY-FRZ-1A-16D",
                Item_Count=1,
            ),
        ]
    )

    return mock_optimization_cluster_data


@pytest.fixture(scope="session")
def mock_df_store_cluster_id_item_counts() -> SparkDataFrame:
    mock_df_store_cluster_id_item_counts_data = spark.createDataFrame(
        [
            Row(STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D", ITEM_NO="678984", Item_Count=1),
            Row(STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D", ITEM_NO="314792", Item_Count=2),
            Row(STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D", ITEM_NO="375039", Item_Count=14),
            Row(STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-16D", ITEM_NO="900100", Item_Count=9),
            Row(STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-16D", ITEM_NO="375030", Item_Count=1),
        ]
    )
    return mock_df_store_cluster_id_item_counts_data


@pytest.fixture(scope="session")
def mock_opt_facings_by_item_and_store_cluster_id() -> SparkDataFrame:
    mock_opt_facings_by_item_and_store_cluster_id_data = spark.createDataFrame(
        [
            Row(
                store_cluster_id="ONT-SBY-FRZ-1A-10D",
                item_no="678984",
                optim_facings_mode=[3.0],
                optim_facings=3,
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                need_state=-1,
            ),
            Row(
                store_cluster_id="ONT-SBY-FRZ-1A-10D",
                item_no="314792",
                optim_facings_mode=[3.0],
                optim_facings=3,
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                need_state=-1,
            ),
            Row(
                store_cluster_id="ONT-SBY-FRZ-1A-10D",
                item_no="375039",
                optim_facings_mode=[1.0],
                optim_facings=1,
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                need_state=1,
            ),
            Row(
                store_cluster_id="ONT-SBY-FRZ-1A-16D",
                item_no="900100",
                optim_facings_mode=[3.0],
                optim_facings=3,
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                need_state=1,
            ),
            Row(
                store_cluster_id="ONT-SBY-FRZ-1A-16D",
                item_no="375030",
                optim_facings_mode=[1.0],
                optim_facings=1,
                region="ontario",
                banner="SOBEYS",
                m_cluster="1A",
                section_master="Frozen Pizza",
                need_state=-1,
            ),
        ]
    )
    return mock_opt_facings_by_item_and_store_cluster_id_data


@pytest.fixture(scope="session")
def mock_final_elastisity_output_sales() -> SparkDataFrame:
    mock_final_elastisity_output_sales_data = spark.createDataFrame(
        [
            Row(
                Region="ontario",
                National_Banner_Desc="SOBEYS",
                Section_Master="Frozen Pizza",
                Item_No="678984",
                Item_Name="DrOetker CasaMama Pizza Deluxe",
                Cannib_Id="CANNIB-930",
                Need_State="-1",
                Cannib_Id_Idx="1",
                Need_State_Idx="1",
                Cluster_Need_State="-1_1A",
                Cluster_Need_State_Idx="1",
                M_Cluster="1A",
                beta=0.7254945039749146,
                facing_fit_1=0.502874493598938,
                facing_fit_2=0.7970371842384338,
                facing_fit_3=1.005748987197876,
                facing_fit_4=1.1676384210586548,
                facing_fit_5=1.2999117374420166,
                facing_fit_6=1.4117472171783447,
                facing_fit_7=1.508623480796814,
                facing_fit_8=1.5940743684768677,
                facing_fit_9=1.6705129146575928,
                facing_fit_10=1.7396599054336548,
                facing_fit_11=1.8027862310409546,
                facing_fit_12=1.8608567714691162,
                facing_fit_0=0.0,
                Margin=-0.03238820284605026,
                Sales=1.0846236944198608,
            ),
            Row(
                Region="ontario",
                National_Banner_Desc="SOBEYS",
                Section_Master="Frozen Pizza",
                Item_No="314792",
                Item_Name="DrOetker Pizza RiCrust 4Cheese",
                Cannib_Id="CANNIB-930",
                Need_State="2",
                Cannib_Id_Idx="1",
                Need_State_Idx="2",
                Cluster_Need_State="2_1B",
                Cluster_Need_State_Idx="2",
                M_Cluster="1A",
                beta=0.8952184319496155,
                facing_fit_1=0.620518147945404,
                facing_fit_2=0.9834979772567749,
                facing_fit_3=1.241036295890808,
                facing_fit_4=1.4407985210418701,
                facing_fit_5=1.6040160655975342,
                facing_fit_6=1.7420146465301514,
                facing_fit_7=1.8615543842315674,
                facing_fit_8=1.9669959545135498,
                facing_fit_9=2.06131649017334,
                facing_fit_10=2.1466400623321533,
                facing_fit_11=2.224534273147583,
                facing_fit_12=2.296190023422241,
                facing_fit_0=0.0,
                Margin=3.4485697746276855,
                Sales=17.132686614990234,
            ),
            Row(
                Region="ontario",
                National_Banner_Desc="SOBEYS",
                Section_Master="Frozen Pizza",
                Item_No="900100",
                Item_Name="DrOetker Pizza RiCrust 4Cheese",
                Cannib_Id="CANNIB-930",
                Need_State="2",
                Cannib_Id_Idx="1",
                Need_State_Idx="2",
                Cluster_Need_State="2_2B",
                Cluster_Need_State_Idx="3",
                M_Cluster="1A",
                beta=0.7381638288497925,
                facing_fit_1=0.5116561651229858,
                facing_fit_2=0.810955822467804,
                facing_fit_3=1.0233123302459717,
                facing_fit_4=1.1880288124084473,
                facing_fit_5=1.3226120471954346,
                facing_fit_6=1.4364004135131836,
                facing_fit_7=1.5349684953689575,
                facing_fit_8=1.621911644935608,
                facing_fit_9=1.699684977531433,
                facing_fit_10=1.7700395584106445,
                facing_fit_11=1.8342682123184204,
                facing_fit_12=1.893352746963501,
                facing_fit_0=0.0,
                Margin=0.08823634684085846,
                Sales=0.4508602023124695,
            ),
            Row(
                Region="ontario",
                National_Banner_Desc="SOBEYS",
                Section_Master="Frozen Pizza",
                Item_No="375030",
                Item_Name="DrOetker Pza CasaDMa ClsscCdn",
                Cannib_Id="CANNIB-930",
                Need_State="2",
                Cannib_Id_Idx="1",
                Need_State_Idx="2",
                Cluster_Need_State="2_1A",
                Cluster_Need_State_Idx="4",
                M_Cluster="1A",
                beta=0.8287477493286133,
                facing_fit_1=0.5744441747665405,
                facing_fit_2=0.9104724526405334,
                facing_fit_3=1.148888349533081,
                facing_fit_4=1.3338180780410767,
                facing_fit_5=1.4849166870117188,
                facing_fit_6=1.6126686334609985,
                facing_fit_7=1.7233325242996216,
                facing_fit_8=1.820944905281067,
                facing_fit_9=1.9082622528076172,
                facing_fit_10=1.9872503280639648,
                facing_fit_11=2.0593607425689697,
                facing_fit_12=2.1256959438323975,
                facing_fit_0=0.0,
                Margin=-0.045011721551418304,
                Sales=0.9453763961791992,
            ),
            Row(
                Region="ontario",
                National_Banner_Desc="SOBEYS",
                Section_Master="Frozen Pizza",
                Item_No="375039",
                Item_Name="DrOetker CasaMama Pizza Deluxe",
                Cannib_Id="CANNIB-930",
                Need_State="-1",
                Cannib_Id_Idx="1",
                Need_State_Idx="1",
                Cluster_Need_State="-1_1B",
                Cluster_Need_State_Idx="5",
                M_Cluster="1A",
                beta=0.6085823774337769,
                facing_fit_1=0.4218371510505676,
                facing_fit_2=0.668596088886261,
                facing_fit_3=0.8436743021011353,
                facing_fit_4=0.9794755578041077,
                facing_fit_5=1.0904332399368286,
                facing_fit_6=1.1842466592788696,
                facing_fit_7=1.2655115127563477,
                facing_fit_8=1.337192177772522,
                facing_fit_9=1.4013127088546753,
                facing_fit_10=1.4593167304992676,
                facing_fit_11=1.5122703313827515,
                facing_fit_12=1.5609829425811768,
                facing_fit_0=0.0,
                Margin=0.0940382108092308,
                Sales=1.2238709926605225,
            ),
        ]
    )
    return mock_final_elastisity_output_sales_data


@pytest.fixture(scope="session")
def mock_df_add() -> SparkDataFrame:
    mock_df_add_data = spark.createDataFrame(
        [
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="164849",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Pizza",
                Need_State=-1,
                Optim_Facings=3,
                Item_Count=2,
                Optim_Facings_One_More=4,
                facing_fit_list=[
                    0.0,
                    0.502874493598938,
                    0.7970371842384338,
                    1.005748987197876,
                    1.1676384210586548,
                    1.2999117374420166,
                    1.4117472171783447,
                    1.508623480796814,
                    1.5940743684768677,
                    1.6705129146575928,
                    1.7396599054336548,
                    1.8027862310409546,
                    1.8608567714691162,
                ],
                optim_sales=1.005748987197876,
                optim_one_more_sales=1.1676384210586548,
                sales_diff=0.1618894338607788,
            ),
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="314792",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Pizza",
                Need_State=-1,
                Optim_Facings=3,
                Item_Count=2,
                Optim_Facings_One_More=4,
                facing_fit_list=[
                    0.0,
                    0.502874493598938,
                    0.7970371842384338,
                    1.005748987197876,
                    1.1676384210586548,
                    1.2999117374420166,
                    1.4117472171783447,
                    1.508623480796814,
                    1.5940743684768677,
                    1.6705129146575928,
                    1.7396599054336548,
                    1.8027862310409546,
                    1.8608567714691162,
                ],
                optim_sales=1.005748987197876,
                optim_one_more_sales=1.1676384210586548,
                sales_diff=0.1618894338607788,
            ),
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="375030",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Pizza",
                Need_State=1,
                Optim_Facings=1,
                Item_Count=1,
                Optim_Facings_One_More=2,
                facing_fit_list=[
                    0.0,
                    0.5495210886001587,
                    0.8709703087806702,
                    1.0990421772003174,
                    1.275948405265808,
                    1.420491337776184,
                    1.5427006483078003,
                    1.6485631465911865,
                    1.7419406175613403,
                    1.8254694938659668,
                    1.9010305404663086,
                    1.9700124263763428,
                    2.0334696769714355,
                ],
                optim_sales=0.5495210886001587,
                optim_one_more_sales=0.8709703087806702,
                sales_diff=0.3214492201805115,
            ),
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="375039",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Pizza",
                Need_State=1,
                Optim_Facings=3,
                Item_Count=2,
                Optim_Facings_One_More=4,
                facing_fit_list=[
                    0.0,
                    0.5495210886001587,
                    0.8709703087806702,
                    1.0990421772003174,
                    1.275948405265808,
                    1.420491337776184,
                    1.5427006483078003,
                    1.6485631465911865,
                    1.7419406175613403,
                    1.8254694938659668,
                    1.9010305404663086,
                    1.9700124263763428,
                    2.0334696769714355,
                ],
                optim_sales=1.0990421772003174,
                optim_one_more_sales=1.275948405265808,
                sales_diff=0.17690622806549072,
            ),
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="405921",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Pizza",
                Need_State=-1,
                Optim_Facings=1,
                Item_Count=None,
                Optim_Facings_One_More=2,
                facing_fit_list=[
                    0.0,
                    0.502874493598938,
                    0.7970371842384338,
                    1.005748987197876,
                    1.1676384210586548,
                    1.2999117374420166,
                    1.4117472171783447,
                    1.508623480796814,
                    1.5940743684768677,
                    1.6705129146575928,
                    1.7396599054336548,
                    1.8027862310409546,
                    1.8608567714691162,
                ],
                optim_sales=0.502874493598938,
                optim_one_more_sales=0.7970371842384338,
                sales_diff=0.29416269063949585,
            ),
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="576782",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Handhelds",
                Need_State=0,
                Optim_Facings=2,
                Item_Count=14,
                Optim_Facings_One_More=3,
                facing_fit_list=[
                    0.0,
                    1.162400484085083,
                    1.8423610925674438,
                    2.324800968170166,
                    2.6990103721618652,
                    3.0047616958618164,
                    3.263270616531372,
                    3.487201452255249,
                    3.6847221851348877,
                    3.8614108562469482,
                    4.021245002746582,
                    4.16716194152832,
                    4.301393032073975,
                ],
                optim_sales=1.8423610925674438,
                optim_one_more_sales=2.324800968170166,
                sales_diff=0.48243987560272217,
            ),
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="678984",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Pizza",
                Need_State=2,
                Optim_Facings=2,
                Item_Count=None,
                Optim_Facings_One_More=3,
                facing_fit_list=[
                    0.0,
                    0.5744441747665405,
                    0.9104724526405334,
                    1.148888349533081,
                    1.3338180780410767,
                    1.4849166870117188,
                    1.6126686334609985,
                    1.7233325242996216,
                    1.820944905281067,
                    1.9082622528076172,
                    1.9872503280639648,
                    2.0593607425689697,
                    2.1256959438323975,
                ],
                optim_sales=0.9104724526405334,
                optim_one_more_sales=1.148888349533081,
                sales_diff=0.2384158968925476,
            ),
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="819649",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Pizza",
                Need_State=2,
                Optim_Facings=2,
                Item_Count=5,
                Optim_Facings_One_More=3,
                facing_fit_list=[
                    0.0,
                    0.5744441747665405,
                    0.9104724526405334,
                    1.148888349533081,
                    1.3338180780410767,
                    1.4849166870117188,
                    1.6126686334609985,
                    1.7233325242996216,
                    1.820944905281067,
                    1.9082622528076172,
                    1.9872503280639648,
                    2.0593607425689697,
                    2.1256959438323975,
                ],
                optim_sales=0.9104724526405334,
                optim_one_more_sales=1.148888349533081,
                sales_diff=0.2384158968925476,
            ),
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="900100",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Pizza",
                Need_State=-1,
                Optim_Facings=1,
                Item_Count=None,
                Optim_Facings_One_More=2,
                facing_fit_list=[
                    0.0,
                    0.502874493598938,
                    0.7970371842384338,
                    1.005748987197876,
                    1.1676384210586548,
                    1.2999117374420166,
                    1.4117472171783447,
                    1.508623480796814,
                    1.5940743684768677,
                    1.6705129146575928,
                    1.7396599054336548,
                    1.8027862310409546,
                    1.8608567714691162,
                ],
                optim_sales=0.502874493598938,
                optim_one_more_sales=0.7970371842384338,
                sales_diff=0.29416269063949585,
            ),
            Row(
                STORE_CLUSTER_ID="ONT-SBY-FRZ-1A-10D",
                Item_No="955963",
                Region="ontario",
                Banner="SOBEYS",
                Section_Master="Frozen Pizza",
                Need_State=1,
                Optim_Facings=2,
                Item_Count=None,
                Optim_Facings_One_More=3,
                facing_fit_list=[
                    0.0,
                    0.5495210886001587,
                    0.8709703087806702,
                    1.0990421772003174,
                    1.275948405265808,
                    1.420491337776184,
                    1.5427006483078003,
                    1.6485631465911865,
                    1.7419406175613403,
                    1.8254694938659668,
                    1.9010305404663086,
                    1.9700124263763428,
                    2.0334696769714355,
                ],
                optim_sales=0.8709703087806702,
                optim_one_more_sales=1.0990421772003174,
                sales_diff=0.22807186841964722,
            ),
        ]
    )
    return mock_df_add_data


@pytest.fixture(scope="session")
def mock_df_removal() -> SparkDataFrame:
    mock_df_removal_data = spark.createDataFrame()
    return mock_df_removal_data


def test_create_store_cluster_id_item_counts(
    mock_optimization_cluster: SparkDataFrame, mock_bay_data_prefix: SparkDataFrame
):
    df_store_cluster_id_item_counts = create_store_cluster_id_item_counts(
        df_bay_data_pre_index=mock_bay_data_prefix,
        df_opt_output_with_store_cluster_id=mock_optimization_cluster,
    )

    msg = f"count is not matching"
    assert df_store_cluster_id_item_counts.count() == 5, msg

    # checking the schema
    msg = f"schema is not matching"
    assert set(df_store_cluster_id_item_counts.columns) == {
        "ITEM_NO",
        "Item_Count",
        "STORE_CLUSTER_ID",
    }, msg


def test_preprocess_removals(
    mock_df_store_cluster_id_item_counts: SparkDataFrame,
    mock_final_elastisity_output_sales: SparkDataFrame,
    mock_opt_facings_by_item_and_store_cluster_id: SparkDataFrame,
):
    df_removal = preprocess_removals(
        df_opt=mock_opt_facings_by_item_and_store_cluster_id,
        df_elas=mock_final_elastisity_output_sales,
        df_item_counts=mock_df_store_cluster_id_item_counts,
    )

    msg = f"count is not matching"
    assert df_removal.count() == 5, msg

    # checking the schema
    msg = f"schema is not matching"
    assert set(df_removal.columns) == {
        "Banner",
        "Item_Count",
        "Item_No",
        "Need_State",
        "Optim_Facings",
        "Optim_Facings_One_Less",
        "Region",
        "STORE_CLUSTER_ID",
        "Section_Master",
        "facing_fit_list",
        "optim_one_less_sales",
        "optim_sales",
        "sales_diff",
    }, msg


def test_preprocess_additions(
    mock_df_store_cluster_id_item_counts: SparkDataFrame,
    mock_final_elastisity_output_sales: SparkDataFrame,
    mock_opt_facings_by_item_and_store_cluster_id: SparkDataFrame,
):
    df_add = preprocess_additions(
        df_opt=mock_opt_facings_by_item_and_store_cluster_id,
        df_elas=mock_final_elastisity_output_sales,
        df_item_counts=mock_df_store_cluster_id_item_counts,
    )

    msg = f"count is not matching"
    assert df_add.count() == 5, msg

    # checking the schema
    msg = f"schema is not matching"
    assert set(df_add.columns) == {
        "Banner",
        "Item_Count",
        "Item_No",
        "Need_State",
        "Optim_Facings",
        "Optim_Facings_One_More",
        "Region",
        "STORE_CLUSTER_ID",
        "Section_Master",
        "facing_fit_list",
        "optim_one_more_sales",
        "optim_sales",
        "sales_diff",
    }, msg


def test_get_addition_rankings(mock_df_add: SparkDataFrame):
    udf_to_test = get_addition_rankings_udf()

    pdf_expected = pd.DataFrame(
        [
            {
                "REGION": "ontario",
                "BANNER": "SOBEYS",
                "STORE_CLUSTER_ID": "ONT-SBY-FRZ-1A-10D",
                "SECTION_MASTER": "Frozen Pizza",
                "ITEM_TO_ADD": 375030,
                "ORDER": 1,
            },
            {
                "REGION": "ontario",
                "BANNER": "SOBEYS",
                "STORE_CLUSTER_ID": "ONT-SBY-FRZ-1A-10D",
                "SECTION_MASTER": "Frozen Pizza",
                "ITEM_TO_ADD": 405921,
                "ORDER": 2,
            },
            {
                "REGION": "ontario",
                "BANNER": "SOBEYS",
                "STORE_CLUSTER_ID": "ONT-SBY-FRZ-1A-10D",
                "SECTION_MASTER": "Frozen Pizza",
                "ITEM_TO_ADD": 819649,
                "ORDER": 3,
            },
        ]
    )

    # call the udf underlying function
    # NOTE: we are not calling the udf using the standard Spark way
    # we are just calling the underlying function
    pdf_add = mock_df_add.filter(
        (F.col("REGION") == "ontario")
        & (F.col("BANNER") == "SOBEYS")
        & (F.col("STORE_CLUSTER_ID") == "ONT-SBY-FRZ-1A-10D")
        & (F.col("SECTION_MASTER") == "Frozen Pizza")
    ).toPandas()

    pdf_res: pd.DataFrame = udf_to_test.func(pdf_add)

    assert set(pdf_res.columns) == set(pdf_expected.columns), "wrong columns"


def test_get_removal_rankings(mock_df_add: SparkDataFrame):
    udf_to_test = get_removal_rankings_udf()

    pdf_expected = pd.DataFrame(
        [
            {
                "REGION": "ontario",
                "BANNER": "SOBEYS",
                "STORE_CLUSTER_ID": "ONT-SBY-FRZ-1A-10D",
                "SECTION_MASTER": "Frozen Pizza",
                "ITEM_TO_REMOVE": 375030,
                "ORDER": 1,
            },
            {
                "REGION": "ontario",
                "BANNER": "SOBEYS",
                "STORE_CLUSTER_ID": "ONT-SBY-FRZ-1A-10D",
                "SECTION_MASTER": "Frozen Pizza",
                "ITEM_TO_REMOVE": 405921,
                "ORDER": 2,
            },
            {
                "REGION": "ontario",
                "BANNER": "SOBEYS",
                "STORE_CLUSTER_ID": "ONT-SBY-FRZ-1A-10D",
                "SECTION_MASTER": "Frozen Pizza",
                "ITEM_TO_REMOVE": 819649,
                "ORDER": 3,
            },
        ]
    )

    # call the udf underlying function
    # NOTE: we are not calling the udf using the standard Spark way
    # we are just calling the underlying function
    pdf_add = mock_df_add.filter(
        (F.col("REGION") == "ontario")
        & (F.col("BANNER") == "SOBEYS")
        & (F.col("STORE_CLUSTER_ID") == "ONT-SBY-FRZ-1A-10D")
        & (F.col("SECTION_MASTER") == "Frozen Pizza")
    ).toPandas()

    pdf_res: pd.DataFrame = udf_to_test.func(pdf_add)

    assert set(pdf_res.columns) == set(pdf_expected.columns), "wrong columns"
