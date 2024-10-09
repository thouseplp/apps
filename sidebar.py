import streamlit as st
from streamlit_option_menu import option_menu

# Inject custom CSS for the header
def sidebar():
    st.markdown(
        """
        <style>
        header.st-emotion-cache-1qv137k.eczjsme2 {
            padding-top: 20px;  /* Adjust this for more top padding */
            font-size: 18px;  /* Adjust the size of the header text */
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

# Your original pages dictionary
    pages = {
        "Data Upload": [
            st.Page("Targets.py", title="🎯 Closer Targets"),
        ],
        "Appointments": [
            st.Page("pages/1_Web_Appointments.py", title="🌐 Web"),
            st.Page("pages/2_FM_Appointments.py", title="🚪 Field"),
        ],
    }

    pg = st.navigation(pages)
    pg.run()