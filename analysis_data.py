from pyspark.sql.functions import count, countDistinct
from main import menu_df, sales_df


# Total Amount spend by each customer

total_spend_by_customer = (sales_df.join(menu_df,"product_id").groupBy('customer_id').agg({'price':'sum'}).orderBy('customer_id'))
# total_spend_by_customer.show()

# Total amount spend by each food category 

total_spend_by_category = (sales_df.join(menu_df,"product_id").groupBy('product_name').agg({'price':'sum'}).orderBy('product_name'))
# total_spend_by_category.show()

# total amount of sales by month

total_spend_by_month = (sales_df.join(menu_df,"product_id").groupBy('order_month').agg({'price':'sum'}).orderBy('order_month'))
# total_spend_by_month.show()  

# total amount of sales by year

total_spend_by_year = (sales_df.join(menu_df,"product_id").groupBy('order_year').agg({'price':'sum'}).orderBy('order_year'))
# total_spend_by_year.show()  

# total amount of sales by quarter

total_spend_by_quarter = (sales_df.join(menu_df,"product_id").groupBy('order_qaurter').agg({'price':'sum'}).orderBy('order_qaurter'))
# total_spend_by_quarter.show()  

# how many times each product purchased? 

count_of_product_purchase = (sales_df
                                .join(menu_df,"product_id")
                                .groupBy('product_id','product_name')
                                .agg(count('product_id').alias('prouduct_count'))
                                .orderBy('product_id', 'product_name'))
# count_of_product_purchase.show() 

# top 5 ordered item

top_five_ordered_items = (sales_df
                                .join(menu_df,"product_id")
                                .groupBy('product_id','product_name')
                                .agg(count('product_id').alias('prouduct_count'))
                                .orderBy('prouduct_count',ascending = 0)
                                .drop('product_id')
                                .limit(5)
)
# top_five_ordered_items.show() 

# Top ordere item

top_ordered_item = (sales_df
                                .join(menu_df,"product_id")
                                .groupBy('product_id','product_name')
                                .agg(count('product_id').alias('prouduct_count'))
                                .orderBy('prouduct_count',ascending = 0)
                                .drop('product_id')
                                .limit(1)
)
# top_ordered_item.show() 

# frequency of vistors at the resturant
df=(sales_df.filter(sales_df.source_order=='Restaurant').groupBy('customer_id').agg(countDistinct('order_date'))
)

df.show()
filtered_count = sales_df.filter(sales_df.source_order == 'Restaurant').count()

