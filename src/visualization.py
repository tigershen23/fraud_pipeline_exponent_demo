"""
Visualization Module

This module handles generating visualizations for transaction data
and risk assessment results.
"""

import os
import duckdb
import polars as pl
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from rich.console import Console

console = Console()

class VisualizationGenerator:
    """Class for generating visualizations from transaction data."""
    
    def __init__(self, db_path="database/transactions.duckdb", output_dir="outputs"):
        """
        Initialize the VisualizationGenerator.
        
        Args:
            db_path (str): Path to the DuckDB database file
            output_dir (str): Directory to save visualizations
        """
        self.db_path = db_path
        self.output_dir = output_dir
        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)
        # Initialize the connection
        self.conn = duckdb.connect(db_path)
    
    def generate_transaction_time_series(self):
        """
        Generate time series visualization for transaction volumes and amounts.
        
        Returns:
            str: Path to the saved visualization
        """
        console.print("[bold cyan]Generating transaction time series visualization...[/bold cyan]")
        
        # Query daily transaction data
        daily_data = self.conn.execute("""
            SELECT 
                CAST(timestamp AS DATE) as date,
                COUNT(*) as transaction_count,
                ROUND(AVG(amount), 2) as avg_amount,
                SUM(amount) as total_amount
            FROM transactions
            GROUP BY CAST(timestamp AS DATE)
            ORDER BY date;
        """).fetchdf()
        
        # Create plotly figure with two y-axes
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add traces
        fig.add_trace(
            go.Scatter(
                x=daily_data['date'],
                y=daily_data['transaction_count'],
                name="Transaction Count",
                line=dict(color='royalblue', width=2)
            ),
            secondary_y=False,
        )
        
        fig.add_trace(
            go.Scatter(
                x=daily_data['date'],
                y=daily_data['avg_amount'],
                name="Average Amount ($)",
                line=dict(color='firebrick', width=2, dash='dash')
            ),
            secondary_y=True,
        )
        
        # Set titles
        fig.update_layout(
            title_text="Daily Transaction Volume and Average Amount",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            template="plotly_white",
            height=500,
            width=900
        )
        
        # Set axis titles
        fig.update_xaxes(title_text="Date")
        fig.update_yaxes(title_text="Transaction Count", secondary_y=False)
        fig.update_yaxes(title_text="Average Amount ($)", secondary_y=True)
        
        # Save the figure
        output_path = os.path.join(self.output_dir, "transaction_time_series.html")
        fig.write_html(output_path)
        
        # Also save as PNG for embedding in documentation
        output_png_path = os.path.join(self.output_dir, "transaction_time_series.png")
        fig.write_image(output_png_path)
        
        console.print(f"[green]Transaction time series visualization saved to {output_path}[/green]")
        return output_path
    
    def generate_risk_distribution(self):
        """
        Generate visualization for risk level distribution.
        
        Returns:
            str: Path to the saved visualization
        """
        console.print("[bold cyan]Generating risk distribution visualization...[/bold cyan]")
        
        # Query risk distribution data
        risk_data = self.conn.execute("""
            SELECT 
                risk_level,
                COUNT(*) as transaction_count
            FROM risk_scores
            GROUP BY risk_level
            ORDER BY 
                CASE 
                    WHEN risk_level = 'Very High' THEN 1
                    WHEN risk_level = 'High' THEN 2
                    WHEN risk_level = 'Medium' THEN 3
                    WHEN risk_level = 'Low' THEN 4
                END;
        """).fetchdf()
        
        # Create custom color scale
        colors = {
            'Very High': 'darkred',
            'High': 'red',
            'Medium': 'orange',
            'Low': 'green'
        }
        
        # Map colors to risk levels
        bar_colors = [colors[level] for level in risk_data['risk_level']]
        
        # Create bar chart
        fig = go.Figure(data=[
            go.Bar(
                x=risk_data['risk_level'],
                y=risk_data['transaction_count'],
                marker_color=bar_colors,
                text=risk_data['transaction_count'],
                textposition='auto',
            )
        ])
        
        # Update layout
        fig.update_layout(
            title='Transaction Risk Level Distribution',
            xaxis_title='Risk Level',
            yaxis_title='Number of Transactions',
            template='plotly_white',
            height=500,
            width=800
        )
        
        # Save the figure
        output_path = os.path.join(self.output_dir, "risk_distribution.html")
        fig.write_html(output_path)
        
        # Also save as PNG for embedding in documentation
        output_png_path = os.path.join(self.output_dir, "risk_distribution.png")
        fig.write_image(output_png_path)
        
        console.print(f"[green]Risk distribution visualization saved to {output_path}[/green]")
        return output_path
    
    def generate_transaction_by_hour_heatmap(self):
        """
        Generate heatmap of transaction counts by hour and day of week.
        
        Returns:
            str: Path to the saved visualization
        """
        console.print("[bold cyan]Generating transaction heatmap by hour and day...[/bold cyan]")
        
        # Query hourly transaction data
        hourly_data = self.conn.execute("""
            SELECT 
                EXTRACT(HOUR FROM timestamp) as hour,
                EXTRACT(DOW FROM timestamp) as day_of_week,
                COUNT(*) as transaction_count
            FROM transactions
            GROUP BY EXTRACT(HOUR FROM timestamp), EXTRACT(DOW FROM timestamp)
            ORDER BY day_of_week, hour;
        """).fetchdf()
        
        # Reshape data for heatmap
        # Create a pivot table
        pivot_df = hourly_data.pivot_table(
            values='transaction_count', 
            index='day_of_week', 
            columns='hour', 
            fill_value=0
        )
        
        # Day names
        day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        
        # Create heatmap
        fig = px.imshow(
            pivot_df,
            labels=dict(x="Hour of Day", y="Day of Week", color="Transaction Count"),
            x=[f"{h:02d}:00" for h in range(24)],
            y=day_names,
            color_continuous_scale="Viridis",
            aspect="auto"
        )
        
        # Update layout
        fig.update_layout(
            title='Transaction Activity by Hour and Day of Week',
            template='plotly_white',
            height=500,
            width=900
        )
        
        # Save the figure
        output_path = os.path.join(self.output_dir, "transaction_heatmap.html")
        fig.write_html(output_path)
        
        # Also save as PNG for embedding in documentation
        output_png_path = os.path.join(self.output_dir, "transaction_heatmap.png")
        fig.write_image(output_png_path)
        
        console.print(f"[green]Transaction heatmap visualization saved to {output_path}[/green]")
        return output_path
    
    def generate_amount_distribution(self):
        """
        Generate visualization for transaction amount distribution by risk level.
        
        Returns:
            str: Path to the saved visualization
        """
        console.print("[bold cyan]Generating transaction amount distribution by risk level...[/bold cyan]")
        
        # Query transaction amount data
        amount_data = self.conn.execute("""
            SELECT 
                risk_level,
                amount
            FROM risk_scores
            ORDER BY risk_level;
        """).fetchdf()
        
        # Create box plot
        fig = px.box(
            amount_data, 
            x="risk_level", 
            y="amount", 
            color="risk_level",
            category_orders={"risk_level": ["Low", "Medium", "High", "Very High"]},
            color_discrete_map={
                "Low": "green",
                "Medium": "orange",
                "High": "red",
                "Very High": "darkred"
            },
            title="Transaction Amount Distribution by Risk Level"
        )
        
        # Update layout
        fig.update_layout(
            xaxis_title="Risk Level",
            yaxis_title="Transaction Amount ($)",
            template="plotly_white",
            height=500,
            width=800,
            showlegend=False
        )
        
        # Save the figure
        output_path = os.path.join(self.output_dir, "amount_distribution.html")
        fig.write_html(output_path)
        
        # Also save as PNG for embedding in documentation
        output_png_path = os.path.join(self.output_dir, "amount_distribution.png")
        fig.write_image(output_png_path)
        
        console.print(f"[green]Amount distribution visualization saved to {output_path}[/green]")
        return output_path
    
    def close(self):
        """Close the DuckDB connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            console.print("[green]DuckDB connection closed[/green]")

def generate_visualizations(db_path="database/transactions.duckdb", output_dir="outputs"):
    """
    Generate visualizations for transaction data and risk assessment.
    
    Args:
        db_path (str): Path to the DuckDB database file
        output_dir (str): Directory to save visualizations
    
    Returns:
        list: Paths to the generated visualizations
    """
    viz_generator = VisualizationGenerator(db_path, output_dir)
    
    try:
        # Generate visualizations
        paths = []
        
        # 1. Transaction time series
        time_series_path = viz_generator.generate_transaction_time_series()
        paths.append(time_series_path)
        
        # 2. Risk distribution
        risk_dist_path = viz_generator.generate_risk_distribution()
        paths.append(risk_dist_path)
        
        # 3. Transaction heatmap
        heatmap_path = viz_generator.generate_transaction_by_hour_heatmap()
        paths.append(heatmap_path)
        
        # 4. Amount distribution
        amount_dist_path = viz_generator.generate_amount_distribution()
        paths.append(amount_dist_path)
        
        # Close connection
        viz_generator.close()
        
        console.print("[bold green]All visualizations generated successfully[/bold green]")
        return paths
    
    except Exception as e:
        console.print(f"[red]Error generating visualizations: {str(e)}[/red]")
        viz_generator.close()
        return []

if __name__ == "__main__":
    # Generate visualizations when run directly
    generate_visualizations()