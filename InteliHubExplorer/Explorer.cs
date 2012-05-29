using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace InteliHubExplorer
{
    public partial class Explorer : Form
    {
        InteliHubClient.Connection _connection;
        string m_server = "10.255.232.50";
        Int16 m_port = 2605;

        public Explorer()
        {
            InitializeComponent();

            serverTextBox.Text = m_server;
            portTextBox.Text = m_port.ToString();
        }

        private void connectButton_Click(object sender, EventArgs e)
        {
            if (_connection != null)
                _connection.Disconnect();

            _connection = new InteliHubClient.Connection(m_server, m_port);

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
            new ContainerViewer(m_server, m_port, containersListView.SelectedItems[0].Text).Show();
        }

        private void containersListView_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                new ContainerViewer(m_server, m_port, containersListView.SelectedItems[0].Text).Show();
            }
        }

        private void serverTextBox_TextChanged(object sender, EventArgs e)
        {
            m_server = serverTextBox.Text;
        }

        private void portTextBox_TextChanged(object sender, EventArgs e)
        {
            try
            {
                m_port = Convert.ToInt16(portTextBox.Text);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
            }
        }
    }
}
