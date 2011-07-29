using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace TioExplorer
{
    public partial class Explorer : Form
    {
        TioClient.Connection _connection;

        public Explorer()
        {
            InitializeComponent();
        }

        private void connectButton_Click(object sender, EventArgs e)
        {
            if (_connection != null)
                _connection.Disconnect();

            _connection = new TioClient.Connection(serverTextBox.Text, Convert.ToInt16(portTextBox.Text));

            statusLabel.Text = "connected!";

            LoadContainerList();
        }

        private void LoadContainerList()
        {

        }

    }
}
