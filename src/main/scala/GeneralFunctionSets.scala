import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

object GeneralFunctionSets {
    // 输入一个字符串的时间,计算时间戳(单位MS)
    // 输入如 2019-06-15 21:12:46
    // 返回一个时间戳,如1615546989
    def transTimeToTimestamp(timeString: String): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString).getTime / 1000
        time
    }
    // 输入一个时间戳,得对应的字符串
    // 输入如:1615546989(单位MS)
    // 返回一个字符串,如2019-06-15 21:12:46
    def transTimeToString(timeStamp: Long): String = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.format(timeStamp * 1000)
        time
    }
    // 输入一个字符串的时间,是一个月里面的哪一天
    // 输入如 2019-06-15 21:12:46
    // 返回一个日期的日,如 15
    def dayOfMonth_string(timeString: String): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.DAY_OF_MONTH)
    }

    // 输入一个时间戳,返回是一个月里面的哪一天
    // 输入如1615546989
    // 返回一个日期的日,如 15
    def dayOfMonth_long(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.DAY_OF_MONTH)
    }

    def hourOfDay(timeString: String): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.HOUR_OF_DAY)
    }

    def hourOfDay_long(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.HOUR_OF_DAY)
    }

    // 获取时间戳对应当天的秒数
    def secondsOfDay(t: Long): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        val H = calendar.get(Calendar.HOUR_OF_DAY)
        val M = calendar.get(Calendar.MINUTE)
        val S = calendar.get(Calendar.SECOND)
        (H * 60 + M) * 60 + S
    }

    // 获取以半小时为单位的时间段序号当月的第几个半小时，0为起点
    def halfHourOfDay(t: Long): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        (calendar.get(Calendar.DAY_OF_MONTH) - 1) * 48 + calendar.get(Calendar.HOUR_OF_DAY) * 2 + calendar.get(Calendar.MINUTE) / 30
    }
}
