from abnamro_caap_featurestore_engine.feature_store import transform, FeatureStore, psf


@transform(
    name="Raw_SABFR_sabfr_cust_party_to_party_v2",
    requires=[
    	"Ingested_SABFR_sabfr_cust_party_to_party_v2:cligrmcli",
		"Ingested_SABFR_sabfr_cust_party_to_party_v2:cligrmreg",
		"Ingested_SABFR_sabfr_cust_party_to_party_v2:cligrmrel",
		"Ingested_SABFR_sabfr_cust_party_to_party_v2:libelle",
		"Ingested_SABFR_sabfr_cust_party_to_party_v2:cligrmdde",
		"Ingested_SABFR_sabfr_cust_party_to_party_v2:cligrmdfi",
    ],
    write_type="upsert_from_full",
    write_partition="status",
    write_metadata="default_raw",
    owner="mail_car_caap_data_enabling@nl.abnamro.com",   
)
def transform_8c3640852dcc4e328ac5b5bc9149e5f7(context: FeatureStore):
    df_dict = context.load_requirements()

    result = df_dict["Ingested_SABFR_sabfr_cust_party_to_party_v2"]
    result = result.dropDuplicates()

    return context.save(result)
          
