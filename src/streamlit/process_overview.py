import streamlit as st
import graphviz

def pictorial_overview():

  st.title(f":blue[ Process Overview : ]")

  st.subheader("Model Training")

  st.graphviz_chart('''
  digraph {
      label="Model Training"
      subgraph cluster_a {
        label=""
        bgcolor="mintcream"
        fontsize="30pt"
        fontname="Courier New"
                    
        "Generate Sensor Readings (10 Days)" [style=filled, fillcolor=lightblue]
        "Ingest Anomalies Manually (3 sets for 3 diff days)" [style=filled, fillcolor=lightblue]
        "Create Supervised ML Model with Cortex" [style=filled, fillcolor=lightblue] 
                      
        "Generate Sensor Readings (10 Days)"->"Ingest Anomalies Manually (3 sets for 3 diff days)"->"Create Supervised ML Model with Cortex"
      }
                    
  }
  }
  ''')

  st.divider()

  st.subheader("Model Inference")
  st.graphviz_chart('''
  digraph {
      label = "Model Inference"
      subgraph cluster_b {
        label=""
        bgcolor="floralwhite"
        fontsize="40pt"
        fontname="Courier New"
                    
        
        "Ingest Anomalies (3 sets on Day=10)" [style=filled, fillcolor=lightblue]
        "Use Model To Detect Anomalies" [style=filled, fillcolor=lightblue] 
        "Use TopInsights To Detect Contributing Features" [style=filled, fillcolor=lightblue] 
                      
        "Ingest Anomalies (3 sets on Day=10)"->
        "Use Model To Detect Anomalies"->
        "Use TopInsights To Detect Contributing Features"
      }
  }
  }
  ''')