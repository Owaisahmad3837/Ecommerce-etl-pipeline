import pandas as pd 


class Pipleline:
  def load_data(self):
    products=pd.read_csv("data/products.csv")
    orders=pd.read_csv("data/orders.csv")
    users=pd.read_csv("data/users.csv")
    payments=pd.read_csv("data/payments.csv")

    return orders, users, products, payments
  
  def orders(self,orders):
    orders=orders.drop_duplicates()
    orders["quantity"]=orders["quantity"].fillna(1)
    orders["order_date"]=pd.to_datetime(orders["order_date"])
    return orders
  
  def users(self,users):
    users=users.drop_duplicates()
    users['name']=users['name'].fillna("Unknown")
    users['email']=users['email'].fillna("notpresent")
    return users
  
  def products(self,products):
    products=products.drop_duplicates()
    products["price"]=products["price"].fillna(products["price"].median())
    products["product_name"]=products["product_name"].fillna("udate wait")
    return products



  def payments(self,payments):
    payments=payments.drop_duplicates()
    payments["amount"]=payments["amount"].fillna(0)
    return payments
    

  def merge_data(self,orders, users, products, payments):
    merge=orders.merge(users,on="user_id")
    med_merge=merge.merge(products,on="product_id")
    final_merge=med_merge.merge(payments,on="order_id")
    return final_merge
  
  def calculate_revenue(self,data):
    data["revenue"] = data["quantity"] * data["price"]
    return data
  

  def top_product(self,data):
    top=data.groupby("product_name")["quantity"].sum().reset_index()
    top=top.sort_values("quantity",ascending=False)
    return top
  

  def monthly(self,data):
    data["month"] = data["order_date"].dt.to_period("M")
    monthly_sales = data.groupby("month")["revenue"].sum().reset_index()
    return monthly_sales
  
  def save_data(self,top_products,monthly_sales):
    top_products.to_csv("output/top_products.csv", index=False)
    monthly_sales.to_csv("output/monthly_sales.csv", index=False)

def main():
  p=Pipleline()
  orders,users,products,payments=p.load_data()

  orders=p.orders(orders)
  users=p.users(users)
  products=p.products(products)
  payments=p.payments(payments)

  full_data=p.merge_data(orders,users, products, payments)

  data=p.calculate_revenue(full_data)
  top_products=p.top_product(data)
  monthly_sales=p.monthly(data)

  p.save_data(top_products, monthly_sales)  

if __name__ == "__main__":
    main()