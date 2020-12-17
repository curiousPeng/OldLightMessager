namespace LightMessager.Common
{
    internal static class MessageIdHelper
    {
        public static long GenerateMessageIdFrom(string str)
        {
            return str.GetHashCode();
        }
    }
}
