from tests.utils.spark_util import spark_tests as spark

from pyspark import Row


DF_SUPPLIER_ITEM_MAPPING = spark.createDataFrame(
    [
        Row(ITEM_NO="499756", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="411919",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="410504",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="679871", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="594672", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="641483", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="152818",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(ITEM_NO="257601", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="269683", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="679872", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="679133", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="176916", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="679796",
            SUPPLIER_ID="200808",
            SUPPLIER_NM="Tree of Life Canada Inc",
        ),
        Row(
            ITEM_NO="760057",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="649728", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="771940",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="946796", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="679126", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="860156",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="508104", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(
            ITEM_NO="679873", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="634765", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="641479", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="14925", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="461389",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="448131", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="501901", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="679794",
            SUPPLIER_ID="200808",
            SUPPLIER_NM="Tree of Life Canada Inc",
        ),
        Row(ITEM_NO="323181", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="230487", SUPPLIER_ID="296134", SUPPLIER_NM="Cafe Cimo Inc"),
        Row(
            ITEM_NO="286333",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="607905", SUPPLIER_ID="342570", SUPPLIER_NM="Balzacs Coffee Ltd."),
        Row(
            ITEM_NO="359440",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="281466", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="607068", SUPPLIER_ID="342570", SUPPLIER_NM="Balzacs Coffee Ltd."),
        Row(ITEM_NO="389355", SUPPLIER_ID="256023", SUPPLIER_NM="ABC Cork Co"),
        Row(
            ITEM_NO="955391",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="38099", SUPPLIER_ID="202373", SUPPLIER_NM="I D Foods Corporation"),
        Row(
            ITEM_NO="323405",
            SUPPLIER_ID="200808",
            SUPPLIER_NM="Tree of Life Canada Inc",
        ),
        Row(ITEM_NO="257603", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="126590",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="620822",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="655971", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="561806",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="302956", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="595481", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="256477", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="389354", SUPPLIER_ID="256023", SUPPLIER_NM="ABC Cork Co"),
        Row(
            ITEM_NO="483909",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="320722", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="760036",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(
            ITEM_NO="375532",
            SUPPLIER_ID="212581",
            SUPPLIER_NM="Casa Cubana / Spike Marks Inc",
        ),
        Row(
            ITEM_NO="577385",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="260311", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="649769", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="771935",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="302953", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="71126", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="641480", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="569121",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="299508", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="678812", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="679111", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="508482", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="499761", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="595434", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="308998",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="956026",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="596466",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="456801", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(ITEM_NO="756261", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="634501", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="677745", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="771933",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="299504", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="843926",
            SUPPLIER_ID="200184",
            SUPPLIER_NM="Saputo Dairy Prod Can GP - Edmonton",
        ),
        Row(
            ITEM_NO="679117", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="819537", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="607631",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="256066", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="660795", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="493685",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="518001", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="544312", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="667883", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="642984",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="499641", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="771913",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="649771", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="589701", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(
            ITEM_NO="758405",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(
            ITEM_NO="399212",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="260924", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="257598", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="342532",
            SUPPLIER_ID="287103",
            SUPPLIER_NM="OUGHTRED COFFEE & TEA LTD",
        ),
        Row(ITEM_NO="607446", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="675983",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="286331",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="390589",
            SUPPLIER_ID="255217",
            SUPPLIER_NM="Husky Food Imp & Distributors Ltd",
        ),
        Row(ITEM_NO="658080", SUPPLIER_ID="256023", SUPPLIER_NM="ABC Cork Co"),
        Row(
            ITEM_NO="171131",
            SUPPLIER_ID="267424",
            SUPPLIER_NM="Sobeys Global Sourcing Inc",
        ),
        Row(
            ITEM_NO="286381", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="594660", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="359363", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="771925",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="681123", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="360222", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="260302", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="518092", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="525180",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="605502", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="660065", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="607609",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="268848",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="681124", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="375638", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="243181", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="376846", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="812034", SUPPLIER_ID="322390", SUPPLIER_NM="Salem Brothers"),
        Row(
            ITEM_NO="688099", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="949853", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="299506", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="679791",
            SUPPLIER_ID="200808",
            SUPPLIER_NM="Tree of Life Canada Inc",
        ),
        Row(
            ITEM_NO="858647",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="194578",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="303379", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="377709", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="621185", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="439177",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="952436", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="482440", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="577417",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="551491",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="431099", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="613024", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="652389", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="505976", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="642318", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="607867",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="193028",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="583398", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="607090", SUPPLIER_ID="342570", SUPPLIER_NM="Balzacs Coffee Ltd."),
        Row(
            ITEM_NO="171132",
            SUPPLIER_ID="267424",
            SUPPLIER_NM="Sobeys Global Sourcing Inc",
        ),
        Row(ITEM_NO="758393", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(
            ITEM_NO="827139",
            SUPPLIER_ID="290293",
            SUPPLIER_NM="Massimo Zanetti Beverages USA - Cdn",
        ),
        Row(ITEM_NO="224798", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="577406",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="683790", SUPPLIER_ID="295244", SUPPLIER_NM="Cafe Agga Vip Inc"),
        Row(
            ITEM_NO="494846",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="286335",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="551492",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="260304", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="302957", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="683529", SUPPLIER_ID="202373", SUPPLIER_NM="I D Foods Corporation"
        ),
        Row(
            ITEM_NO="577418",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="464691", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="199290", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="843922",
            SUPPLIER_ID="200184",
            SUPPLIER_NM="Saputo Dairy Prod Can GP - Edmonton",
        ),
        Row(ITEM_NO="505975", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="860157",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="652392", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="675979",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="499638", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="503382", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="411932",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="607632",
            SUPPLIER_ID="308373",
            SUPPLIER_NM="PARADISE MOUNTAIN ORGANIC",
        ),
        Row(ITEM_NO="137271", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="256469", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="302633", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="956025",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="708325", SUPPLIER_ID="342570", SUPPLIER_NM="Balzacs Coffee Ltd."),
        Row(
            ITEM_NO="688102", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="343614", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="759045", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="681121", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="679150", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="281463", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="589887", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(
            ITEM_NO="860141",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="819687", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="505977", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="755887", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="361975", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="949870", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="379625", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="649745", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="547950", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="257602", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="456807", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(
            ITEM_NO="193027",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="374933",
            SUPPLIER_ID="212581",
            SUPPLIER_NM="Casa Cubana / Spike Marks Inc",
        ),
        Row(ITEM_NO="589683", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(
            ITEM_NO="607636",
            SUPPLIER_ID="308373",
            SUPPLIER_NM="PARADISE MOUNTAIN ORGANIC",
        ),
        Row(
            ITEM_NO="955652",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="955685",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="589811", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(
            ITEM_NO="675970",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="667973", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="679154", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="649751", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="651140", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="837478",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="299568",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="755886", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="301122", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="903361",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="158628",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="903337",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="343153", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="320726",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="764774",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="641492", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="589851", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(ITEM_NO="681111", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="411935",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="842236",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="411931",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="342530",
            SUPPLIER_ID="287103",
            SUPPLIER_NM="OUGHTRED COFFEE & TEA LTD",
        ),
        Row(
            ITEM_NO="577424",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="589696", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(
            ITEM_NO="679142", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="365201",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="716917", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="607076", SUPPLIER_ID="342570", SUPPLIER_NM="Balzacs Coffee Ltd."),
        Row(
            ITEM_NO="577427",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="410501",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="679130", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="858648",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="197118", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="818759", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="949845", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="308987",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="269687", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="664758",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="505970", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="675971",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="550200",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="550544",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="390132",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="755888", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="360657", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="756272", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="956029",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="679119", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="874545", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="151899",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="596479",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="504608",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="679798",
            SUPPLIER_ID="200808",
            SUPPLIER_NM="Tree of Life Canada Inc",
        ),
        Row(ITEM_NO="649753", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="675968",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="525179",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="572144", SUPPLIER_ID="295244", SUPPLIER_NM="Cafe Agga Vip Inc"),
        Row(ITEM_NO="290928", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="212226",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="818746", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="336066", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="600081", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="400489", SUPPLIER_ID="295244", SUPPLIER_NM="Cafe Agga Vip Inc"),
        Row(
            ITEM_NO="915051", SUPPLIER_ID="202373", SUPPLIER_NM="I D Foods Corporation"
        ),
        Row(ITEM_NO="941999", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="860124",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(
            ITEM_NO="827619",
            SUPPLIER_ID="352680",
            SUPPLIER_NM="Murchies Tea & Coffee (2007) Ltd",
        ),
        Row(ITEM_NO="641478", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="302954", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="551496",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="684042", SUPPLIER_ID="295244", SUPPLIER_NM="Cafe Agga Vip Inc"),
        Row(
            ITEM_NO="480873", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="756259", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="667890", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="771919",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(
            ITEM_NO="771929",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="424472", SUPPLIER_ID="256023", SUPPLIER_NM="ABC Cork Co"),
        Row(
            ITEM_NO="323406",
            SUPPLIER_ID="200808",
            SUPPLIER_NM="Tree of Life Canada Inc",
        ),
        Row(ITEM_NO="464692", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="200417",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="358751", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="641902", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="505985", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="530059", SUPPLIER_ID="256132", SUPPLIER_NM="Excelsior Foods Inc"),
        Row(ITEM_NO="465075", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="837477",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="456804", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(
            ITEM_NO="515218", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="657925", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="465070", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="652388", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="621186", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="589326", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="521203",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="619853", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="680142", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="634771", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="641888",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="532930", SUPPLIER_ID="200780", SUPPLIER_NM="Pastene Inc"),
        Row(
            ITEM_NO="675965",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="758392", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(ITEM_NO="515216", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="756260", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="708326", SUPPLIER_ID="342570", SUPPLIER_NM="Balzacs Coffee Ltd."),
        Row(
            ITEM_NO="462064", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="390181",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="286386", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="505982", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="159170", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="343631", SUPPLIER_ID="256132", SUPPLIER_NM="Excelsior Foods Inc"),
        Row(
            ITEM_NO="303849", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="302955", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="396229", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="521211",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(
            ITEM_NO="323407",
            SUPPLIER_ID="200808",
            SUPPLIER_NM="Tree of Life Canada Inc",
        ),
        Row(
            ITEM_NO="200419",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="594667", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="651142", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="860144",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="158625", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="675981",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="377710", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="256655", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="949849", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="503309", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(
            ITEM_NO="483912",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="860154",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(
            ITEM_NO="342521",
            SUPPLIER_ID="287103",
            SUPPLIER_NM="OUGHTRED COFFEE & TEA LTD",
        ),
        Row(ITEM_NO="573284", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="955686",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="681119", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="936112",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="785899", SUPPLIER_ID="296134", SUPPLIER_NM="Cafe Cimo Inc"),
        Row(
            ITEM_NO="377700", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="667884", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="256657", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="551495",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="955392",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="506055", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="493727",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="952434", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="684041", SUPPLIER_ID="295244", SUPPLIER_NM="Cafe Agga Vip Inc"),
        Row(ITEM_NO="290925", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="299507", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="827137",
            SUPPLIER_ID="290293",
            SUPPLIER_NM="Massimo Zanetti Beverages USA - Cdn",
        ),
        Row(ITEM_NO="343617", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="903403",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="295360", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="365199",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="649774", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="600444", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(ITEM_NO="649765", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="503312", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="629777",
            SUPPLIER_ID="200808",
            SUPPLIER_NM="Tree of Life Canada Inc",
        ),
        Row(
            ITEM_NO="486899",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="342518",
            SUPPLIER_ID="287103",
            SUPPLIER_NM="OUGHTRED COFFEE & TEA LTD",
        ),
        Row(ITEM_NO="946874", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="594668", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="688395", SUPPLIER_ID="202373", SUPPLIER_NM="I D Foods Corporation"
        ),
        Row(
            ITEM_NO="594663", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="811586", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="544452", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="544304", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="400582", SUPPLIER_ID="295244", SUPPLIER_NM="Cafe Agga Vip Inc"),
        Row(
            ITEM_NO="399217",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="606093", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="657846", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="660118", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="260309", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="365202",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="942052", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="577386",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="607870",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="360322", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="359437",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="286648",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="323408",
            SUPPLIER_ID="200808",
            SUPPLIER_NM="Tree of Life Canada Inc",
        ),
        Row(
            ITEM_NO="656274",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="493687",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="606667",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="681112", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="544257", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="518117", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="379634", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="812033", SUPPLIER_ID="322390", SUPPLIER_NM="Salem Brothers"),
        Row(
            ITEM_NO="755743",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="235584", SUPPLIER_ID="326323", SUPPLIER_NM="Italcan Imports Inc"),
        Row(ITEM_NO="613011", SUPPLIER_ID="326323", SUPPLIER_NM="Italcan Imports Inc"),
        Row(
            ITEM_NO="620834",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="290927", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="881818", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="934331",
            SUPPLIER_ID="345064",
            SUPPLIER_NM="Cafe ONeill Le Moussonneur, Micro",
        ),
        Row(
            ITEM_NO="903346",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="176898", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="641496", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="291165", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="273718", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="951842",
            SUPPLIER_ID="347695",
            SUPPLIER_NM="Bellucci Distributions Ltd",
        ),
        Row(ITEM_NO="952431", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="493723",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="375541",
            SUPPLIER_ID="212581",
            SUPPLIER_NM="Casa Cubana / Spike Marks Inc",
        ),
        Row(ITEM_NO="681116", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="344599", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="410502",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="286649",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="767836",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="667681",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="279653", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(ITEM_NO="681544", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="582717", SUPPLIER_ID="296134", SUPPLIER_NM="Cafe Cimo Inc"),
        Row(ITEM_NO="35700", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(
            ITEM_NO="951841",
            SUPPLIER_ID="347695",
            SUPPLIER_NM="Bellucci Distributions Ltd",
        ),
        Row(
            ITEM_NO="684819", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="951838",
            SUPPLIER_ID="347695",
            SUPPLIER_NM="Bellucci Distributions Ltd",
        ),
        Row(
            ITEM_NO="375643", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="679157", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="342528",
            SUPPLIER_ID="287103",
            SUPPLIER_NM="OUGHTRED COFFEE & TEA LTD",
        ),
        Row(
            ITEM_NO="411909",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="675969",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="506056", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="679136", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="517961", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="544261", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="452970", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="600461", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(
            ITEM_NO="493729",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="256471", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="607119", SUPPLIER_ID="342570", SUPPLIER_NM="Balzacs Coffee Ltd."),
        Row(
            ITEM_NO="286379", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="342516",
            SUPPLIER_ID="287103",
            SUPPLIER_NM="OUGHTRED COFFEE & TEA LTD",
        ),
        Row(
            ITEM_NO="323176",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="358740", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="290926", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="771937",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(ITEM_NO="634499", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="551082",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="256467", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="955393",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="466916", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="758391", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(
            ITEM_NO="680138", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="667885", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="951839",
            SUPPLIER_ID="235416",
            SUPPLIER_NM="Primary Vendor (Error Vendor)",
        ),
        Row(ITEM_NO="949859", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="949864", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="212227",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="281465", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="432395", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(
            ITEM_NO="320699",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="589890", SUPPLIER_ID="274373", SUPPLIER_NM="Club Coffee"),
        Row(ITEM_NO="425582", SUPPLIER_ID="256023", SUPPLIER_NM="ABC Cork Co"),
        Row(
            ITEM_NO="679146", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="551494",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="827617",
            SUPPLIER_ID="352680",
            SUPPLIER_NM="Murchies Tea & Coffee (2007) Ltd",
        ),
        Row(
            ITEM_NO="377724", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="149448", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="256470", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="280087",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="499762", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="605505", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="466919", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="936109",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="400491", SUPPLIER_ID="295244", SUPPLIER_NM="Cafe Agga Vip Inc"),
        Row(
            ITEM_NO="500466",
            SUPPLIER_ID="338229",
            SUPPLIER_NM="GREEN WORLD FOOD EXPRESS INC",
        ),
        Row(ITEM_NO="273719", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="681554", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="281462", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="365217",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="951837",
            SUPPLIER_ID="347695",
            SUPPLIER_NM="Bellucci Distributions Ltd",
        ),
        Row(ITEM_NO="505971", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="594674", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="662431", SUPPLIER_ID="295244", SUPPLIER_NM="Cafe Agga Vip Inc"),
        Row(
            ITEM_NO="525178",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="767834",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="159351", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(
            ITEM_NO="688321",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(
            ITEM_NO="494837",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="667882", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="607598",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(
            ITEM_NO="683532", SUPPLIER_ID="202373", SUPPLIER_NM="I D Foods Corporation"
        ),
        Row(ITEM_NO="523066", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="594666", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="212224",
            SUPPLIER_ID="203933",
            SUPPLIER_NM="Smucker Foods of Canada",
        ),
        Row(ITEM_NO="784910", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="900777",
            SUPPLIER_ID="202203",
            SUPPLIER_NM="General Mills Canada Inc",
        ),
        Row(
            ITEM_NO="437059",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(ITEM_NO="586128", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="437054",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="637170", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(
            ITEM_NO="191992", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(
            ITEM_NO="608825",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="229482",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="329576",
            SUPPLIER_ID="202203",
            SUPPLIER_NM="General Mills Canada Inc",
        ),
        Row(
            ITEM_NO="642386", SUPPLIER_ID="202518", SUPPLIER_NM="UNFI Canada - Concord"
        ),
        Row(
            ITEM_NO="900775",
            SUPPLIER_ID="202203",
            SUPPLIER_NM="General Mills Canada Inc",
        ),
        Row(
            ITEM_NO="437075",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="329047",
            SUPPLIER_ID="202203",
            SUPPLIER_NM="General Mills Canada Inc",
        ),
        Row(
            ITEM_NO="437095",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="437063",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="229490",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="229489",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="574861", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(
            ITEM_NO="437057",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="437079",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="778779", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(
            ITEM_NO="146495",
            SUPPLIER_ID="201510",
            SUPPLIER_NM="Janes Family Foods Ltd - Sofina",
        ),
        Row(
            ITEM_NO="840905",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(ITEM_NO="630816", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="437073",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(ITEM_NO="586114", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="57598",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(ITEM_NO="190403", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="437093",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="791590", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(ITEM_NO="190404", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="229480",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="334127",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="758737",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="325157",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="778764", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(
            ITEM_NO="232134",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(ITEM_NO="707835", SUPPLIER_ID="201595", SUPPLIER_NM="Les Oeufs Bec-O Inc"),
        Row(ITEM_NO="623917", SUPPLIER_ID="306873", SUPPLIER_NM="MTY Franchising Inc"),
        Row(
            ITEM_NO="437060",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(ITEM_NO="660154", SUPPLIER_ID="201595", SUPPLIER_NM="Les Oeufs Bec-O Inc"),
        Row(ITEM_NO="375596", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="229487",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(ITEM_NO="510751", SUPPLIER_ID="201595", SUPPLIER_NM="Les Oeufs Bec-O Inc"),
        Row(
            ITEM_NO="465491",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="916887",
            SUPPLIER_ID="289696",
            SUPPLIER_NM="Trifuno Retail Solutions Inc",
        ),
        Row(ITEM_NO="707833", SUPPLIER_ID="201595", SUPPLIER_NM="Les Oeufs Bec-O Inc"),
        Row(ITEM_NO="660153", SUPPLIER_ID="201595", SUPPLIER_NM="Les Oeufs Bec-O Inc"),
        Row(
            ITEM_NO="777942", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(ITEM_NO="190406", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="458340", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(ITEM_NO="630810", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="437052",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="458343", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(
            ITEM_NO="840907",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(ITEM_NO="707832", SUPPLIER_ID="201595", SUPPLIER_NM="Les Oeufs Bec-O Inc"),
        Row(
            ITEM_NO="437076",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(ITEM_NO="586112", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="896912",
            SUPPLIER_ID="202203",
            SUPPLIER_NM="General Mills Canada Inc",
        ),
        Row(
            ITEM_NO="458351", SUPPLIER_ID="200491", SUPPLIER_NM="McCain Foods  (Canada)"
        ),
        Row(
            ITEM_NO="608841",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="437068",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="900776",
            SUPPLIER_ID="202203",
            SUPPLIER_NM="General Mills Canada Inc",
        ),
        Row(
            ITEM_NO="224869",
            SUPPLIER_ID="202175",
            SUPPLIER_NM="E D Smith & Sons Limited",
        ),
        Row(
            ITEM_NO="896909",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="465492",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(ITEM_NO="190405", SUPPLIER_ID="202112", SUPPLIER_NM="Cavendish Farms"),
        Row(
            ITEM_NO="955562",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="955560",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(
            ITEM_NO="437086",
            SUPPLIER_ID="201531",
            SUPPLIER_NM="Kellogg Canada Inc H4504",
        ),
        Row(ITEM_NO="946878", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="461997", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="263256",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(
            ITEM_NO="679140",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(
            ITEM_NO="607638",
            SUPPLIER_ID="308373",
            SUPPLIER_NM="PARADISE MOUNTAIN ORGANIC",
        ),
        Row(ITEM_NO="652387", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="903366",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="649737", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="358748", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="461661", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="641482", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="784911", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="946877", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="681120", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="341633", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="342529",
            SUPPLIER_ID="287103",
            SUPPLIER_NM="OUGHTRED COFFEE & TEA LTD",
        ),
        Row(
            ITEM_NO="544288", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="566621", SUPPLIER_ID="212417", SUPPLIER_NM="Melitta Canada Inc"),
        Row(ITEM_NO="363912", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="582953", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="784914", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="609908", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="613908",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(
            ITEM_NO="286767", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(
            ITEM_NO="404338",
            SUPPLIER_ID="200458",
            SUPPLIER_NM="Santa Maria - Sofina Foods Inc",
        ),
        Row(ITEM_NO="500582", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(
            ITEM_NO="286807", SUPPLIER_ID="200505", SUPPLIER_NM="Kraft Heinz Canada ULC"
        ),
        Row(ITEM_NO="504054", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="760043",
            SUPPLIER_ID="202099",
            SUPPLIER_NM="Mother Parkers Tea & Coffee Inc",
        ),
        Row(
            ITEM_NO="827613",
            SUPPLIER_ID="352680",
            SUPPLIER_NM="Murchies Tea & Coffee (2007) Ltd",
        ),
        Row(ITEM_NO="400492", SUPPLIER_ID="295244", SUPPLIER_NM="Cafe Agga Vip Inc"),
        Row(ITEM_NO="642023", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="683507", SUPPLIER_ID="202373", SUPPLIER_NM="I D Foods Corporation"
        ),
        Row(ITEM_NO="522139", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="819536", SUPPLIER_ID="201894", SUPPLIER_NM="Keurig Canada Inc"),
        Row(ITEM_NO="413372", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="934337",
            SUPPLIER_ID="345064",
            SUPPLIER_NM="Cafe ONeill Le Moussonneur, Micro",
        ),
        Row(ITEM_NO="784912", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(
            ITEM_NO="200418",
            SUPPLIER_ID="200318",
            SUPPLIER_NM="Thomas Large & Singer Ontario",
        ),
        Row(ITEM_NO="585052", SUPPLIER_ID="296134", SUPPLIER_NM="Cafe Cimo Inc"),
        Row(
            ITEM_NO="607635",
            SUPPLIER_ID="308373",
            SUPPLIER_NM="PARADISE MOUNTAIN ORGANIC",
        ),
        Row(ITEM_NO="159168", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
        Row(ITEM_NO="881819", SUPPLIER_ID="203518", SUPPLIER_NM="Nestle Canada Inc"),
    ]
)
