using System;
using System.Drawing;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;
using System.Data;
using System.IO;

namespace Mercury
{
	/// <summary>
	/// Summary description for Form1.
	/// </summary>
	public class Mercury : System.Windows.Forms.Form
	{
		private System.Windows.Forms.Button cmdBuildCat;
		private System.Windows.Forms.StatusBar sbStatus;
		private System.Windows.Forms.StatusBarPanel sbpStatus;
		private System.Windows.Forms.TextBox txtSearch;
		private System.Windows.Forms.ListBox lstResults;
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;

		Catalog _cat;

		delegate long AddCatalogItemDelegate(String name, String path);
		delegate void UpdateProgressDelegate(long catId);

		AddCatalogItemDelegate _addCatItem;
		UpdateProgressDelegate _updateProgress;

		DateTime _started;
		private System.Windows.Forms.StatusBarPanel spbPerformance;
		int _itemsAdded;

		public Mercury()
		{
			//
			// Required for Windows Form Designer support
			//
			InitializeComponent();

			_cat = new Catalog();
			_addCatItem = new AddCatalogItemDelegate(DoAddCatalogItem);
			_updateProgress = new UpdateProgressDelegate(UpdateBuildProgress);
		}

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		protected override void Dispose( bool disposing )
		{
			if( disposing )
			{
				if (components != null) 
				{
					components.Dispose();
				}
			}
			base.Dispose( disposing );
		}

		#region Windows Form Designer generated code
		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			this.cmdBuildCat = new System.Windows.Forms.Button();
			this.sbStatus = new System.Windows.Forms.StatusBar();
			this.sbpStatus = new System.Windows.Forms.StatusBarPanel();
			this.txtSearch = new System.Windows.Forms.TextBox();
			this.lstResults = new System.Windows.Forms.ListBox();
			this.spbPerformance = new System.Windows.Forms.StatusBarPanel();
			((System.ComponentModel.ISupportInitialize)(this.sbpStatus)).BeginInit();
			((System.ComponentModel.ISupportInitialize)(this.spbPerformance)).BeginInit();
			this.SuspendLayout();
			// 
			// cmdBuildCat
			// 
			this.cmdBuildCat.Location = new System.Drawing.Point(8, 8);
			this.cmdBuildCat.Name = "cmdBuildCat";
			this.cmdBuildCat.TabIndex = 0;
			this.cmdBuildCat.Text = "Build Cat";
			this.cmdBuildCat.Click += new System.EventHandler(this.cmdBuildCat_Click);
			// 
			// sbStatus
			// 
			this.sbStatus.Location = new System.Drawing.Point(0, 392);
			this.sbStatus.Name = "sbStatus";
			this.sbStatus.Panels.AddRange(new System.Windows.Forms.StatusBarPanel[] {
																						this.sbpStatus,
																						this.spbPerformance});
			this.sbStatus.ShowPanels = true;
			this.sbStatus.Size = new System.Drawing.Size(640, 22);
			this.sbStatus.TabIndex = 1;
			// 
			// sbpStatus
			// 
			this.sbpStatus.AutoSize = System.Windows.Forms.StatusBarPanelAutoSize.Spring;
			this.sbpStatus.Text = "Idle";
			this.sbpStatus.Width = 524;
			// 
			// txtSearch
			// 
			this.txtSearch.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
				| System.Windows.Forms.AnchorStyles.Right)));
			this.txtSearch.Location = new System.Drawing.Point(8, 48);
			this.txtSearch.Name = "txtSearch";
			this.txtSearch.Size = new System.Drawing.Size(616, 20);
			this.txtSearch.TabIndex = 2;
			this.txtSearch.Text = "";
			this.txtSearch.TextChanged += new System.EventHandler(this.txtSearch_TextChanged);
			// 
			// lstResults
			// 
			this.lstResults.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
				| System.Windows.Forms.AnchorStyles.Left) 
				| System.Windows.Forms.AnchorStyles.Right)));
			this.lstResults.Location = new System.Drawing.Point(8, 80);
			this.lstResults.Name = "lstResults";
			this.lstResults.Size = new System.Drawing.Size(616, 290);
			this.lstResults.TabIndex = 3;
			// 
			// Mercury
			// 
			this.AutoScaleBaseSize = new System.Drawing.Size(5, 13);
			this.ClientSize = new System.Drawing.Size(640, 414);
			this.Controls.Add(this.lstResults);
			this.Controls.Add(this.txtSearch);
			this.Controls.Add(this.sbStatus);
			this.Controls.Add(this.cmdBuildCat);
			this.Name = "Mercury";
			this.Text = "Mercury Test";
			((System.ComponentModel.ISupportInitialize)(this.sbpStatus)).EndInit();
			((System.ComponentModel.ISupportInitialize)(this.spbPerformance)).EndInit();
			this.ResumeLayout(false);

		}
		#endregion

		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main() 
		{
			Application.Run(new Mercury());
		}

		private void cmdBuildCat_Click(object sender, System.EventArgs e) {
			_cat.InitCatalog();
			_cat.ClearCatalog();

			Object obj = _cat.BeginBuildCatalog();

			_started = DateTime.Now;
			_itemsAdded = 0;

			LoadFolder(@"S:\Mainline\acs_non_dev");

			_cat.EndBuildCatalog(obj);

			sbpStatus.Text = "Catalog loaded";
			Application.DoEvents();
		}
		
		private void LoadFolder(String path) {
			sbpStatus.Text = "Processing " + path;
			Application.DoEvents();

			AddCatalogItem(Path.GetFileName(path) == null ? path : Path.GetFileName(path), path);
			String[] files = Directory.GetFiles(path);

			foreach (String file in files) {
				AddCatalogItem(Path.GetFileNameWithoutExtension(file), file);
			}

			String[] folders = Directory.GetDirectories(path);

			foreach (String folder in folders) {
				LoadFolder(Path.Combine(path, folder));
			}
		}

		private void txtSearch_TextChanged(object sender, System.EventArgs e) {
			//Update the search results
			String searchTerm = txtSearch.Text;

			SearchResults srs = _cat.SearchCatalog(searchTerm);

			lstResults.Items.Clear();
			foreach (SearchResult sr in srs) {
				lstResults.Items.Add(sr);
			}
		}

		private void AddCatalogItem(String title, String path) {
			//_addCatItem.BeginInvoke(title, path, new AsyncCallback(AddCatalogItemAsyncCallback), null);
			DoAddCatalogItem(title, path);
			_itemsAdded++;
			TimeSpan runTime = DateTime.Now - _started;
			Double itemsPerSecond = (Double)_itemsAdded / runTime.TotalSeconds;

			spbPerformance.Text = String.Format("({0}/s)", itemsPerSecond);
		}

		private long DoAddCatalogItem(String name, String path) {
			return _cat.AddCatalogItem(name, path);
		}

		private void AddCatalogItemAsyncCallback(IAsyncResult ar) {
			long catId = _addCatItem.EndInvoke(ar);

			Invoke(_updateProgress, new object[] {catId});			
		}

		private void UpdateBuildProgress(long catId) {
			sbpStatus.Text = String.Format("Added catalog item {0}", catId);
		}
	}
}
