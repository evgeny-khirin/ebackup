;;;-------------------------------------------------------------------
;;; Copyright (c) 2009-2010 by Evgeny Khirin.
;;;-------------------------------------------------------------------
!include "WordFunc.nsh"

; activate macros WordFunc
!insertmacro WordReplace

; The name of the installer
Name "Erlios Backup"

; The file to write
OutFile "eb_pers-2.5.2.win32.exe"

; The default installation directory
InstallDir "$PROGRAMFILES\ErliosBackup\"

; Registry key to check for directory (so if you install again, it will
; overwrite the old one automatically)
InstallDirRegKey HKLM "Software\ErliosBackup" "Install_Dir"

; Request application privileges for Windows Vista
RequestExecutionLevel admin

;--------------------------------

; Pages
PageEx license
			 LicenseData "LICENSE"
PageExEnd
Page components
Page directory
Page instfiles

UninstPage uninstConfirm
UninstPage instfiles

;---------------------------------------------------------------
; check privileges and OS before install
;---------------------------------------------------------------
Function .onInit
	Push $0
	ClearErrors
	UserInfo::GetAccountType
	IfErrors Win9x
	Pop $0
	StrCmp $0 "Admin" 0 +2
		Goto done
  MessageBox MB_OK "Administrator privileges required to install Erlios Backup"
	Abort
	Win9x:
		# This one means you don't need to care about admin or
		# not admin because Windows 9x doesn't either
		MessageBox MB_OK "Error! Erlios Backup can't run under Windows 9x!"
		Abort
	done:
	Pop $0
FunctionEnd

;---------------------------------------------------------------
; check privileges before uninstall
;---------------------------------------------------------------
Function un.onInit
	Push $0
	UserInfo::GetAccountType
	Pop $0
	StrCmp $0 "Admin" 0 +2
		Goto done
  MessageBox MB_OK "Administrator privileges required to uninstall Erlios Backup"
	Abort
	done:
	Pop $0
FunctionEnd

;---------------------------------------------------------------
; The stuff to install
;---------------------------------------------------------------
Section "Erlios Backup"
  SectionIn RO

	; Install for all users
	SetShellVarContext all

  ; Set output path to the installation directory.
  SetOutPath $INSTDIR

  ; Put files there
  File "LICENSE"
  File "COPYRIGHT"
	File eb_pers.exe
	File eb_pers_win.exe
	File eb_pers.ini
	File /r bin
	File /r erts-5.6.5
	File /r lib
	File /r releases
  CreateDirectory "$INSTDIR\log"
SectionEnd

;---------------------------------------------------------------
; Post install tasks
;---------------------------------------------------------------
Section ""
  ; Write the installation path into the registry
  WriteRegStr HKLM "SOFTWARE\ErliosBackup" "Install_Dir" "$INSTDIR"

  ; Write the uninstall keys for Windows
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\Erlios Backup" "DisplayName" "Erlios Backup"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\Erlios Backup" "UninstallString" '"$INSTDIR\uninstall.exe"'
  WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\Erlios Backup" "NoModify" 1
  WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\Erlios Backup" "NoRepair" 1
  WriteUninstaller "uninstall.exe"

	; Write shell assotiations
	WriteRegStr HKCR "*\shell\Add to Erlios Backup\command" "" '"$INSTDIR\eb_pers_win.exe" ++ -add_source "%1"'
	WriteRegStr HKCR "*\shell\Exclude from Erlios Backup\command" "" '"$INSTDIR\eb_pers_win.exe" ++ -add_exclude "%1"'
	WriteRegStr HKCR "Folder\shell\Add to Erlios Backup\command" "" '"$INSTDIR\eb_pers_win.exe" ++ -add_source "%1"'
	WriteRegStr HKCR "Folder\shell\Exclude from Erlios Backup\command" "" '"$INSTDIR\eb_pers_win.exe" ++ -add_exclude "%1"'

	; Add system tray icon auto run
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Run" "Erlios Backup Tray" '"$INSTDIR\eb_pers_win.exe" ++ -start_tray'
	; Add user mode application auto run
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Run" "Erlios Backup User Mode" '"$INSTDIR\eb_pers_win.exe" ++ -user_app'

	; Create INI files
	${WordReplace} "$INSTDIR\erts-5.6.5\bin" "\" "\\" "+" $R0
	WriteINIStr "$INSTDIR\bin\erl.ini" erlang Bindir $R0
	WriteINIStr "$INSTDIR\erts-5.6.5\bin\erl.ini" erlang Bindir $R0
	WriteINIStr "$INSTDIR\bin\erl.ini" erlang Progname erl
	WriteINIStr "$INSTDIR\erts-5.6.5\bin\erl.ini" erlang Progname erl
	${WordReplace} "$INSTDIR" "\" "\\" "+" $R0
	WriteINIStr "$INSTDIR\bin\erl.ini" erlang Rootdir $R0
	WriteINIStr "$INSTDIR\erts-5.6.5\bin\erl.ini" erlang Rootdir $R0

	; Register disabled service
  ExecWait `"$INSTDIR\erts-5.6.5\bin\erlsrv.exe" add Erlios_Backup \
					 -onfail restart \
					 -machine "$INSTDIR\eb_pers.exe" \
					 -workdir "$INSTDIR" \
					 -sname eb_pers_server@localhost \
					 -args "-sasl sasl_error_logger {file,\\\\\\\"log/eb_pers_server.log\\\\\\\"} -sasl errlog_type error -s eb_pers run_app win_service ++ -service"`
  ExecWait `"$INSTDIR\erts-5.6.5\bin\erlsrv.exe" disable Erlios_Backup`

	; Start user mode application
	ExecWait `"$INSTDIR\eb_pers_win.exe" ++ -user_app`

	; Start system tray icon
  ExecWait `"$INSTDIR\eb_pers_win.exe" ++ -start_tray`

	; Create menus
  CreateDirectory "$SMPROGRAMS\Erlios Backup"
  CreateShortCut "$SMPROGRAMS\Erlios Backup\Admin Console.lnk" "$INSTDIR\eb_pers.url" "" "$INSTDIR\eb_pers.exe" 0
  CreateShortCut "$SMPROGRAMS\Erlios Backup\Uninstall.lnk" "$INSTDIR\uninstall.exe" "" "$INSTDIR\uninstall.exe" 0
SectionEnd

;---------------------------------------------------------------
; Uninstaller
;---------------------------------------------------------------
Section "Uninstall"
	; Uninstall for all users
	SetShellVarContext all

	; Deactivate license key
	ExecWait `"$INSTDIR\eb_pers.exe" ++ -uninstall`

	; Remove system tray icon
	ExecWait `"$INSTDIR\eb_pers.exe" ++ -stop_tray`

	; Delete auto run keys
  DeleteRegValue HKLM "Software\Microsoft\Windows\CurrentVersion\Run" "Erlios Backup Tray"
  DeleteRegValue HKLM "Software\Microsoft\Windows\CurrentVersion\Run" "Erlios Backup User Mode"

	; Stop and remove service
  ExecWait `"$INSTDIR\erts-5.6.5\bin\erlsrv.exe" remove Erlios_Backup`
	; Ensure that server is stopped
	ExecWait `"$INSTDIR\eb_pers.exe" ++ -stop_server`
	; Kill epmd
  ExecWait `"$INSTDIR\erts-5.6.5\bin\epmd.exe" -kill`

  ; Remove registry keys
  DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\Erlios Backup"
  DeleteRegKey HKLM "Software\ErliosBackup"

	; Remove shell assotiations
	DeleteRegKey HKCR "*\shell\Add to Erlios Backup"
	DeleteRegKey HKCR "*\shell\Exclude from Erlios Backup"
	DeleteRegKey HKCR "Folder\shell\Add to Erlios Backup"
	DeleteRegKey HKCR "Folder\shell\Exclude from Erlios Backup"

  ; Remove files and directories
  RMDir /r /REBOOTOK "$SMPROGRAMS\Erlios Backup"
	Delete /REBOOTOK "$INSTDIR\uninstall.exe"
	Delete /REBOOTOK "$INSTDIR\LICENSE"
	Delete /REBOOTOK "$INSTDIR\COPYRIGHT"
	Delete /REBOOTOK "$INSTDIR\eb_pers.exe"
	Delete /REBOOTOK "$INSTDIR\eb_pers_win.exe"
	Delete /REBOOTOK "$INSTDIR\eb_pers.ini"
	Delete /REBOOTOK "$INSTDIR\eb_pers.url"
	Delete /REBOOTOK "$INSTDIR\erl_crash.dump"
	RMDir /r /REBOOTOK "$INSTDIR\bin"
	RMDir /r /REBOOTOK "$INSTDIR\erts-5.6.5"
	RMDir /r /REBOOTOK "$INSTDIR\lib"
	RMDir /r /REBOOTOK "$INSTDIR\releases"
	RMDir /r /REBOOTOK "$INSTDIR\log"
SectionEnd
