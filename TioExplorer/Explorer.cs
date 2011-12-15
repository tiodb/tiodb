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
            containersListView.Items.Clear();

            int count = 0;

            _connection.Open("meta/containers").Query(
                delegate(object key, object value, object metadata)
                {
                    containersListView.Items.Add(
                        new ListViewItem(new string[] { Convert.ToString(key), Convert.ToString(value) }));

                    count++;

                    if (count % 100 == 0)
                    {
                        Application.DoEvents();
                        statusLabel.Text = String.Format("{0} containers", count);
                    }
                });

            statusLabel.Text = String.Format("{0} containers", count);
        }

        

        private void Explorer_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.F5)
                LoadContainerList();
        }

        private void containersListView_DoubleClick(object sender, EventArgs e)
        {
            new ContainerViewer(_connection.Open(containersListView.SelectedItems[0].Text)).Show();
        }

        private void containersListView_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
                new ContainerViewer(_connection.Open(containersListView.SelectedItems[0].Text)).Show();
        }
    }
}
