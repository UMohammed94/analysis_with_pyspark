from main import menu_df, sales_df


# Total Amount spend by each customer

total_spend_by_customer = (sales_df.join(menu_df,"product_id").groupBy('customer_id').agg({'price':'sum'}).orderBy('customer_id'))
total_spend_by_customer.show()

# Total amount spend by each food category 

total_spend_by_category = (sales_df.join(menu_df,"product_id").groupBy('product_name').agg({'price':'sum'}).orderBy('product_name'))
total_spend_by_category.show()

# total amount of sales by month

total_spend_by_month = (sales_df.join(menu_df,"product_id").groupBy('order_month').agg({'price':'sum'}).orderBy('order_month'))
total_spend_by_month.show()  

# total amount of sales by year

total_spend_by_year = (sales_df.join(menu_df,"product_id").groupBy('order_year').agg({'price':'sum'}).orderBy('order_year'))
total_spend_by_year.show()  

# total amount of sales by quarter

total_spend_by_quarter = (sales_df.join(menu_df,"product_id").groupBy('order_qaurter').agg({'price':'sum'}).orderBy('order_qaurter'))
total_spend_by_quarter.show()  