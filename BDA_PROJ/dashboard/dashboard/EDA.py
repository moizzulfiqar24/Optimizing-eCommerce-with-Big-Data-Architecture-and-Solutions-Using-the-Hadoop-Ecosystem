import happybase
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# Function to create and test HBase connection
@st.cache_resource
def get_hbase_connection():
    try:
        connection = happybase.Connection(host='hbasebda', port=9090, transport="buffered", protocol="binary" )
        connection.tables()  # Test connection by listing tables
        return connection
    except Exception as e:
        st.error(f"Failed to connect to HBase: {e}")
        return None

# Function to fetch data from HBase
@st.cache_data
def fetch_data(table_name, column_family, limit=None):
    try:
        connection = get_hbase_connection()
        if connection is None:
            raise Exception("No active HBase connection.")

        table = connection.table(table_name)
        rows = table.scan(limit=limit)
        data = []

        for key, value in rows:
            record = {}
            for col, val in value.items():
                if col.startswith(f"{column_family}:".encode('utf-8')):  # Convert string to bytes
                    col_name = col.split(b":")[1].decode('utf-8')  # Extract the column name
                    record[col_name] = val.decode('utf-8') if isinstance(val, bytes) else val

            data.append(record)

        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Streamlit App
def main():
    st.set_page_config(page_title="Enhanced HBase EDA", layout="wide")

    st.title("HBase EDA Dashboard")
    st.markdown("""<style>
        .stSidebar {
            background-color: #2B3A42;
            color: white;
        }
        h1 {
            color: #1F618D;
        }
        .stDataFrame {
            border: 1px solid #D5DBDB;
            border-radius: 5px;
        }
    </style>""", unsafe_allow_html=True)

    # Sidebar selection
    st.sidebar.title("Navigation")
    selected_table = st.sidebar.selectbox("Select a table for EDA", ["CustomerTable", "ProductTable", "OrderTable"])

    # Fetch data based on selected table
    if selected_table == "CustomerTable":
        data = fetch_data("CustomerTable", "info")
        if data.empty:
            st.warning(f"No data available for {selected_table}.")
        else:
            st.subheader("Customer Table Data")
            st.dataframe(data, use_container_width=True)

            # EDA for CustomerTable
            st.write("#### Age Distribution")
            fig, ax = plt.subplots()
            data['age'].astype(int).value_counts().sort_index().plot(kind='bar', ax=ax, color='skyblue')
            ax.set_title('Age Distribution')
            st.pyplot(fig)

            st.write("#### Gender Distribution")
            fig, ax = plt.subplots()
            data['gender'].value_counts().plot(kind='pie', ax=ax, autopct='%1.1f%%', colors=['pink', 'lightblue'])
            ax.set_ylabel('')
            st.pyplot(fig)

            st.write("#### Location Distribution")
            fig, ax = plt.subplots()
            data['location'].value_counts().head(10).plot(kind='bar', ax=ax, color='green')
            ax.set_title('Top 10 Locations')
            st.pyplot(fig)

    elif selected_table == "ProductTable":
        data_details = fetch_data("ProductTable", "details")
        data_inventory = fetch_data("ProductTable", "inventory")

        if data_details.empty and data_inventory.empty:
            st.warning(f"No data available for {selected_table}.")
        else:
            st.subheader("Product Table Data")
            st.write("#### Details Column Family")
            st.dataframe(data_details, use_container_width=True)
            st.write("#### Inventory Column Family")
            st.dataframe(data_inventory, use_container_width=True)

            # EDA for ProductTable
            st.write("#### Price Distribution")
            if 'price' in data_inventory.columns:
                fig, ax = plt.subplots()
                data_inventory['price'].astype(float).plot(kind='hist', bins=20, ax=ax, color='orange')
                ax.set_title('Price Distribution')
                st.pyplot(fig)
            else:
                st.warning("Price column is missing in the inventory data.")

            st.write("#### Stock Quantity Distribution")
            if 'stock_quantity' in data_inventory.columns:
                fig, ax = plt.subplots()
                data_inventory['stock_quantity'].astype(float).value_counts().sort_index().plot(kind='line', ax=ax, color='blue')
                ax.set_title('Stock Quantity Distribution')
                st.pyplot(fig)
            else:
                st.warning("Stock Quantity column is missing in the inventory data.")

            st.write("#### Category Distribution")
            if 'category' in data_details.columns:
                fig, ax = plt.subplots()
                data_details['category'].value_counts().plot(kind='bar', ax=ax, color='purple')
                ax.set_title('Category Distribution')
                st.pyplot(fig)
            else:
                st.warning("Category column is missing in the details data.")

            # Additional EDA for ProductTable
            st.write("#### Price vs Stock Quantity Analysis")
            if 'price' in data_inventory.columns and 'stock_quantity' in data_inventory.columns:
                fig, ax = plt.subplots()
                ax.scatter(data_inventory['price'].astype(float), data_inventory['stock_quantity'].astype(float), color='red')
                ax.set_title('Price vs Stock Quantity')
                ax.set_xlabel('Price')
                ax.set_ylabel('Stock Quantity')
                st.pyplot(fig)
            else:
                st.warning("Price or Stock Quantity column is missing in the inventory data.")

    elif selected_table == "OrderTable":
        data = fetch_data("OrderTable", "info", limit=100)
        if data.empty:
            st.warning(f"No data available for {selected_table}.")
        else:
            st.subheader("Order Table Data")
            st.dataframe(data, use_container_width=True)

            # EDA for OrderTable
            st.write("#### Total Amount Distribution")
            if 'total_amount' in data.columns:
                fig, ax = plt.subplots()
                data['total_amount'].astype(float).plot(kind='hist', bins=20, ax=ax, color='gold')
                ax.set_title('Total Amount Distribution')
                st.pyplot(fig)
            else:
                st.warning("Total Amount column is missing in the data.")

            st.write("#### Payment Method Distribution")
            if 'payment_method' in data.columns:
                fig, ax = plt.subplots()
                data['payment_method'].value_counts().plot(kind='bar', ax=ax, color='brown')
                ax.set_title('Payment Method Distribution')
                st.pyplot(fig)
            else:
                st.warning("Payment Method column is missing in the data.")

            st.write("#### Quantity Distribution")
            if 'quantity' in data.columns:
                fig, ax = plt.subplots()
                data['quantity'].astype(float).value_counts().sort_index().plot(kind='line', ax=ax, color='blue')
                ax.set_title('Quantity Distribution')
                st.pyplot(fig)
            else:
                st.warning("Quantity column is missing in the data.")

if __name__ == "__main__":
    main()
