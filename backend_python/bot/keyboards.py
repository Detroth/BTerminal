from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def get_settings_keyboard(settings):
    splash_th, adv_en, min_vol, fade_en, mom_en = settings
    
    splash_text = f"🌊 Сплеши: >{splash_th}%" if splash_th < 100 else "🌊 Сплеши: ВЫКЛ"
    adv_text = f"🧠 Advanced: {'ВКЛ' if adv_en else 'ВЫКЛ'}"
    fade_text = f"📉 Fade: {'ВКЛ' if fade_en else 'ВЫКЛ'}"
    mom_text = f"🚀 Momentum: {'ВКЛ' if mom_en else 'ВЫКЛ'}"
    vol_text = f"📊 Объем: >${int(min_vol/1000)}k" if min_vol > 0 else "📊 Объем: Любой"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=splash_text, callback_data="toggle_splash")],
        [InlineKeyboardButton(text=adv_text, callback_data="toggle_advanced")],
        [InlineKeyboardButton(text=fade_text, callback_data="toggle_fade")],
        [InlineKeyboardButton(text=mom_text, callback_data="toggle_momentum")],
        [InlineKeyboardButton(text=vol_text, callback_data="toggle_volume")]
    ])
    return keyboard