"""
ATLAS AI V9.2 - TEST NOTIFICATION PATCH

Bu dosya mevcut V9.1 bot.py'yi güvenli şekilde günceller.
Mevcut tarama/sinyal koduna dokunmaz; yalnızca /test komutunu
gerçek Telegram kanalına test bildirimi gönderecek şekilde değiştirir.

Kullanım:
1) Bu dosyayı mevcut bot.py ile aynı klasöre koy.
2) python atlas_ai_v9_2_patch.py
3) bot.py otomatik yedeklenir.
4) Telegram'da /test yaz.
"""

from pathlib import Path
import re
import shutil
import sys

TARGET = Path("bot.py")
BACKUP = Path("bot_v9_1_backup.py")

NEW_CMD_TEST = '''async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a real Telegram channel test notification."""

    now = datetime.now(timezone.utc).astimezone()
    test_time = now.strftime("%d.%m.%Y %H:%M:%S")

    test_text = (
        "🧪 ATLAS AI V9.2 TEST OK\\n\\n"
        "✅ Bot aktif\\n"
        "✅ Telegram bağlantısı çalışıyor\\n"
        "✅ Komut sistemi çalışıyor\\n"
        f"📡 Signal chat: {SIGNAL_CHAT}\\n"
        "📊 Binance REST: hazır\\n"
        f"🕐 Test zamanı: {test_time}\\n"
        "🟢 Execution: OFF (paper only)\\n\\n"
        "🚀 Kanal bildirimi başarıyla test edildi."
    )

    try:
        await context.bot.send_message(
            chat_id=SIGNAL_CHAT,
            text=test_text
        )

        await update.message.reply_text(
            "✅ TEST bildirimi gönderildi.\\n"
            f"📡 Kanal: {SIGNAL_CHAT}"
        )

    except Exception as e:
        log.exception("Test notification failed: %s", e)

        await update.message.reply_text(
            "❌ TEST başarısız!\\n\\n"
            "Kanal bildirimi gönderilemedi.\\n"
            f"📡 Kanal: {SIGNAL_CHAT}\\n\\n"
            f"Hata: {str(e)}"
        )
'''

def main():
    if not TARGET.exists():
        print("❌ bot.py bulunamadı.")
        print("Bu dosyayı ATLAS AI bot.py ile aynı klasöre koy.")
        sys.exit(1)

    source = TARGET.read_text(encoding="utf-8")

    pattern = re.compile(
        r'async def cmd_test\(update: Update, context: ContextTypes\.DEFAULT_TYPE\):.*?(?=\nasync def cmd_performance)',
        re.S,
    )

    match = pattern.search(source)
    if not match:
        print("❌ Mevcut cmd_test fonksiyonu bulunamadı.")
        sys.exit(1)

    shutil.copy2(TARGET, BACKUP)

    updated = source[:match.start()] + NEW_CMD_TEST.rstrip() + source[match.end():]
    TARGET.write_text(updated, encoding="utf-8")

    print("✅ ATLAS AI V9.2 /test güncellemesi tamamlandı.")
    print(f"✅ Yedek: {BACKUP}")
    print("✅ bot.py güncellendi.")
    print()
    print("Telegram'da bota /test yaz.")
    print("Test mesajı SIGNAL_CHAT kanalına gönderilecek.")

if __name__ == "__main__":
    main()
