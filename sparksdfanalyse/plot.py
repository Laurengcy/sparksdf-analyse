'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@Date: 2019-06-12 17:08:56
@LastEditors: laurengcy
@LastEditTime: 2019-06-12 17:16:38
'''


import plotly.plotly as py
import plotly.graph_objs as go 
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.io as pio

def plotTraces_showNsave(plot_traces, plot_layout, filename, save_size={'width': 600, 'height': 600}):
    fig = dict(
        data = plot_traces,
        layout = plot_layout
    )
    plot(fig)
    pio.write_image(fig, filename +'.png', width=save_size['width'], height=save_size['height'])
    return None