using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace TioExplorer
{
    public partial class ContainerViewer : Form
    {
        TioClient.Container _container;

        public ContainerViewer(TioClient.Container container)
        {
            _container = container;
            
            InitializeComponent();

            Text = container.Name;            
        }

        private bool _updating = false;

        void UpdateData()
        {
            if (_updating)
                return;

            try
            {
                _updating = true;

                //
                // let's check if there is a schema for the value
                // 
                string schema = null;
                try
                {
                    schema =_container.GetProperty("schema");
                }
                catch(Exception){}

                if(!String.IsNullOrEmpty(schema))
                {

                }

                itemsListView.Items.Clear();

                int count = 0;

                _container.Query(
                    delegate(object key, object value, object metadata)
                    {
                        itemsListView.Items.Add(new ListViewItem(new string[] { Convert.ToString(key), Convert.ToString(value) }));

                        count++;

                        if (count % 1000 == 0)
                        {
                            Application.DoEvents();
                            statusLabel.Text = String.Format("{0} records", count);
                        }
                    });

                statusLabel.Text = String.Format("{0} records", count);
            }
            finally
            {
                _updating = false;
            }
        }

        private void ContainerViewer_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.F5)
                UpdateData();
        }

        private void ContainerViewer_Load(object sender, EventArgs e)
        {
            UpdateData();
        }
    }
}
