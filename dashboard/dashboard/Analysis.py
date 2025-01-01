import streamlit as st
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go

def read_local_file(file_path):
    return pd.read_csv(file_path)

def main():
    st.set_page_config(page_title="Sales and Customer Dashboard", layout="wide", initial_sidebar_state="expanded")

    # Dashboard styling
    st.markdown(
        """
        <style>
        body {
            background-color: #1e1e1e;
            color: #ffffff;
        }
        .stMetric {
            background-color: #343a40;
            border-radius: 5px;
            padding: 10px;
            font-size: 18px;
            color: #ffffff;
        }
        .stMarkdown {
            color: #f8f9fa;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.title("Enhanced Sales and Customer Dashboard")

    # Base directory for data
    base_dir = "./csvfiles"
    directories = [
        "age_distribution",
        "gender_sales",
        "high_value_customers",
        "location_sales",
        "monthly_sales",
        "payment_method_distribution",
        "top_products",
        "total_revenue",
    ]

    # Product mapping dictionary
    product_mapping = {
        1: 'Chocolate',
        2: 'Salt',
        3: 'Jam',
        4: 'Pork',
        5: 'Olive Oil',
        6: 'Beef',
        7: 'Honey',
        8: 'Oranges',
        9: 'Ice Cream',
        10: 'Chocolate',
        11: 'Flour',
        12: 'Fish',
        13: 'Chips',
        14: 'Bananas',
        15: 'Eggs',
        16: 'Ice Cream',
        17: 'Milk',
        18: '{%,()',
        19: 'Cheese',
        20: 'Soda',
        21: 'Bread',
        22: 'Juice',
        23: 'Coffee',
        24: 'Olive Oil',
        25: 'Bananas',
        26: 'Eggs',
        27: 'Oranges',
        28: 'Chicken',
        29: 'Oranges',
        30: 'Fish',
        31: 'Apples',
        32: 'Cookies',
        33: 'Crackers',
        34: 'Nuts',
        35: 'Ginger',
        36: 'Flour',
        37: 'Carrots',
        38: 'Soda',
        39: 'Tea',
        40: 'Jam',
        41: 'Milk',
        42: 'Flour',
        43: 'Bananas',
        44: 'Coffee',
        45: 'Beef',
        46: 'Eggs',
        47: 'Spinach',
        48: 'Ginger',
        49: 'Sugar',
        50: 'Olive Oil'
    }

    # Show Monthly Sales and Total Revenue at the top
    try:
        # Monthly Sales
        monthly_sales_path = os.path.join(base_dir, "monthly_sales")
        monthly_sales_file = next((f for f in os.listdir(monthly_sales_path) if f.endswith('.csv')), None)
        if not monthly_sales_file:
            raise FileNotFoundError(f"No CSV file found in {monthly_sales_path}")
        monthly_sales_df = read_local_file(os.path.join(monthly_sales_path, monthly_sales_file))
        
        # Total Revenue
        total_revenue_path = os.path.join(base_dir, "total_revenue")
        total_revenue_file = next((f for f in os.listdir(total_revenue_path) if f.endswith('.csv')), None)
        if not total_revenue_file:
            raise FileNotFoundError(f"No CSV file found in {total_revenue_path}")
        total_revenue_df = read_local_file(os.path.join(total_revenue_path, total_revenue_file))
        
        # Display metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric(label="Total Monthly Revenue", value=f"${monthly_sales_df.iloc[0]['Monthly Revenue']:.2f}")
        with col2:
            st.metric(label="Total Revenue", value=f"${total_revenue_df.iloc[0][0]:.2f}")
    except Exception as e:
        st.error(f"Failed to load Monthly Sales or Total Revenue: {e}")

    for directory in directories:
        if directory in ["monthly_sales", "total_revenue"]:
            continue

        dir_path = os.path.join(base_dir, directory)

        try:
            # Find the first CSV file in the directory
            file_name = next((f for f in os.listdir(dir_path) if f.endswith('.csv')), None)
            if not file_name:
                raise FileNotFoundError(f"No CSV file found in {dir_path}")

            file_path = os.path.join(dir_path, file_name)
            df = read_local_file(file_path)
            
            # Visualize the data
            if directory == "age_distribution":
                st.subheader("Age Distribution")
                fig = px.bar(df, x='age_group', y='count', title="Age Group Distribution", color='count')
                st.plotly_chart(fig)

            elif directory == "gender_sales":
                st.subheader("Gender Sales")
                fig = px.pie(df, names='gender', values='Customer Count', title="Gender Sales Distribution")
                st.plotly_chart(fig)

            elif directory == "high_value_customers":
                st.subheader("High Value Customers")
                fig = px.bar(df, x='customer_id', y='Total Revenue', title="High Value Customers", color='Total Revenue')
                st.plotly_chart(fig)

            elif directory == "location_sales":
                st.subheader("Customer Count by Location")
                fig = px.bar(df, x='location', y='Customer Count', title="Customer Distribution by Location", color='Customer Count')
                st.plotly_chart(fig)

            elif directory == "payment_method_distribution":
                st.subheader("Payment Method Distribution")
                fig = px.pie(df, names='payment_method', values='Number of Orders', title="Payment Method Preferences")
                st.plotly_chart(fig)

            elif directory == "top_products":
                st.subheader("Top Products")
                df["product_id"] = df["product_id"].map(product_mapping)
                fig = px.bar(df, x='product_id', y='Total Quantity', title="Top Products by Quantity", color='Total Quantity')
                st.plotly_chart(fig)

        except Exception as e:
            st.error(f"Failed to read data from {directory}: {e}")

    # Additional visualizations
    # try:
    #     st.subheader("Customer Growth Over Time")
    #     growth_path = os.path.join(base_dir, "customer_growth")
    #     growth_file = next((f for f in os.listdir(growth_path) if f.endswith('growth.csv')), None)
    #     if not growth_file:
    #         raise FileNotFoundError(f"No growth.csv file found in {growth_path}")
    #     growth_df = read_local_file(os.path.join(growth_path, growth_file))
    #     fig = px.line(growth_df, x='Month', y='New Customers', title="Customer Growth")
    #     st.plotly_chart(fig)
    # except Exception as e:
    #     st.error(f"Failed to load additional visualizations: {e}")

if __name__ == "__main__":
    main()
