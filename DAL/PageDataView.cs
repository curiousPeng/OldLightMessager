﻿using System.Collections.Generic;

namespace LightMessager.DAL
{
    internal class PageDataView<T>
    {
        private int _totalRecords;
        private IList<T> _Items;

        public PageDataView()
        {
        }

        public int TotalRecords
        {
            get { return _totalRecords; }
            set { _totalRecords = value; }
        }
        
        public IList<T> Items
        {
            get { return _Items; }
            set { _Items = value; }
        }

        public int CurrentPage { get; set; }
        public int TotalPages { get; set; }
    }
}
