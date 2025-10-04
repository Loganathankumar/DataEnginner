# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests

# Write directly to the app
st.title(f":cup_with_straw: Customize your smoothie!:cup_with_straw:")
st.write(
  """Choose the Fruits you want in your custom Smoothie!
  """)

name_on_order = st.text_input("Name on Smoothie:")
st.write("The name of your smoothie will be", name_on_order)

cnx = st.connection("snowflake")
session = cnx.session()

my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))
# st.dataframe(data=my_dataframe, use_container_width=True)
# st.stop()

# convert snowpark dataframe to pandas dataframe so we can use LOC function
pd_df = my_dataframe.to_pandas()
st.dataframe(pd_df)
st.stop()
                                                                                             
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:', my_dataframe, max_selections=5
)
if ingredients_list:
    
    ingredients_string = ''

    for fruit_chosen in ingredients_list:
      ingredients_string += fruit_chosen + ' '
      st.subheader(fruit_chosen + ' Nutrituion Information')
      smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon")
      st_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

    # st.write(ingredients_string)

    my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order)
            values('"""+ ingredients_string + """','"""+name_on_order+"""')"""
    
    time_to_insert = st.button('Submit Order')
    # st.write(my_insert_stmt)
    # st.stop()

    if time_to_insert:
        session.sql(my_insert_stmt).collect()

        st.success('Your Smoothie is ordered!', icon="✅")


    
